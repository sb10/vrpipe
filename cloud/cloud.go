// Copyright © 2016-2019, 2021 Genome Research Limited
// Author: Sendu Bala <sb10@sanger.ac.uk>.
//
//  This file is part of wr.
//
//  wr is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  wr is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with wr. If not, see <http://www.gnu.org/licenses/>.

/*
Package cloud provides functions to interact with cloud providers, used to
create cloud resources so that you can spawn servers, then delete those
resources when you're done.

Currently implemented providers are OpenStack, with AWS planned for the future.
The implementation of each supported provider is in its own .go file.

It's a pseudo plug-in system in that it is designed so that you can easily add a
go file that implements the methods of the provideri interface, to support a
new cloud provider. On the other hand, there is no dynamic loading of these go
files; they are all imported (they all belong to the cloud package), and the
correct one used at run time. To "register" a new provideri implementation you
must add a case for it to New() and RequiredEnv() and rebuild. The spawn()
method must create a file at the path sentinelFilePath once the system has
finalised its boot up and is fully ready to use. The system should be configured
to allow user fuse mounts.

Please note that the methods in this package are NOT safe to be used by more
than 1 process at a time.

    import "github.com/VertebrateResequencing/wr/cloud"

    // deploy
    provider, err := cloud.New("openstack", "wr-prod-username",
		"/home/username/.wr-production/created_cloud_resources")
    err = provider.Deploy(&cloud.DeployConfig{
        RequiredPorts:  []int{22},
        GatewayIP:      "192.168.64.1",
        CIDR:           "192.168.64.0/18",
        DNSNameServers: [...]string{"8.8.4.4", "8.8.8.8"},
    })

    // spawn a server
    flavor := provider.CheapestServerFlavor(1, 1024, "")
    server, err = provider.Spawn("Ubuntu Xenial", "ubuntu", flavor.ID, 20, 2 * time.Minute, true)
    ctx := context.Background()
    server.WaitUntilReady(ctx, "~/.s3cfg")

    // simplistic way of making the most of the server by running as many
    // commands as possible:
    for _, cmd := range myCmds {
        if server.HasSpaceFor(1, 1024, 1) > 0 {
            server.Allocate(1, 1024, 1)
            go func() {
                server.RunCmd(ctx, cmd, false)
                server.Release(1, 1024, 1)
            }()
        } else {
            break
        }
    }

    // destroy everything created
    provider.TearDown()
*/
package cloud

import (
	"context"
	"encoding/gob"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	"github.com/VertebrateResequencing/wr/internal"
	"github.com/gofrs/uuid"
	"github.com/wtsi-ssg/wr/fs/local"
)

// Err* constants are found in the returned Errors under err.Err, so you can
// cast and check if it's a certain type of error. ErrMissingEnv gets appended
// to with missing environment variable names, so check based on prefix.
var (
	ErrBadProvider     = "unknown provider name"
	ErrMissingEnv      = "missing environment variables: "
	ErrBadResourceName = "your resource name prefix contains disallowed characters"
	ErrNoFlavor        = "no server flavor can meet your resource requirements"
	ErrBadFlavor       = "no server flavor with that id/name exists"
	ErrBadRegex        = "your flavor regular expression was not valid"
	ErrBadCIDR         = "no subnet matches your supplied CIDR"
	ErrNoTearDown      = "nothing to tear down; did you deploy with the current credentials?"
)

// sshTimeOut is how long we wait for ssh to work when an ssh request is made to
// a server.
var sshTimeOut = 5 * time.Minute

// sentinelFilePath is the file that provideri implementers must create on each
// spawn()ed server once it is fully ready to use.
const sentinelFilePath = "/tmp/.wr_cloud_sentinel"

// sentinelInitScript can be used as user data to the cloud-init mechanism to
// create sentinelFilePath. We also try to turn off requiretty in /etc/sudoers,
// to allow postCreationScripts passed to WaitUntilReady() to be run with sudo.
// And we try to enable user_allow_other in fuse.conf to allow user mounts to
// work. And we try to enable system requests to do clean shutdowns (176 means
// s, u and o can be sent to /proc/sysrq-trigger).
var sentinelInitScript = []byte("#!/bin/bash\nsed -i 's/^Defaults\\s*requiretty/Defaults\\t!requiretty/' /etc/sudoers\nsed -i '/user_allow_other/s/^#//g' /etc/fuse.conf\nchmod o+r /etc/fuse.conf\nsudo sysctl -w kernel.sysrq=176\ntouch " + sentinelFilePath)

// sentinelTimeOut is how long we wait for sentinelFilePath to be created before
// we give up and return an error from WaitUntilReady().
var sentinelTimeOut = 10 * time.Minute

// pcsTimeOut is how long we wait for a user's post creation script to exit
// before we give up and return an error from WaitUntilReady().
var pcsTimeOut = 15 * time.Minute

// cleanShutDownCmd should be executed before terminating a server. It forces a
// sync of filesystems, then remounts them read-only. It does not send 'o' to
// /proc/sysrq-trigger to trigger an actual shutdown, because that can
// immediately sever the connection over which you are trying to run this
// command, resulting in you waiting on a timeout instead of knowing if the
// command completed successfully.
// 'sync' on any modern system should wait for writes to complete, and ought to
// be all we really need. Some client/server filesystems may benefit from an
// explicit umount as well, but `umount -a` will tend to just fail because the
// filesystems are busy. We can't generically only umount a particular fs that
// was used for writing job output. Using /proc/sysrq-trigger lets us at least
// request a ro remount (should be equivalent of umount as far as letting fs
// servers know we're done), even if we don't know if it succeeds.
const cleanShutDownCmd = `sync && echo 's' | sudo tee -a /proc/sysrq-trigger > /dev/null && echo 'u' | sudo tee -a /proc/sysrq-trigger > /dev/null`

// defaultDNSNameServers holds some public (google) dns name server addresses
// for use when creating cloud subnets that need internet access.
var defaultDNSNameServers = [...]string{"8.8.4.4", "8.8.8.8"}

// defaultCIDR is a useful range allowing 16384 servers to be spawned, with a
// defaultGateWayIP at the start of that range.
const defaultGateWayIP = "192.168.64.1"
const defaultCIDR = "192.168.64.0/18"

// touchStampFormat is the time format we use for `touch -d`.
const touchStampFormat = "2006-01-02T15:04:05-0700"

// hostNameRegex is used by nameToHostName() to make strings valid hostnames.
var hostNameRegex = regexp.MustCompile(`[^a-z0-9\-]+`)

const openstackName = "openstack"

// Error records an error and the operation and provider caused it.
type Error struct {
	Provider string // the provider's Name
	Op       string // name of the method
	Err      string // one of our Err* vars
}

func (e Error) Error() string {
	return "cloud(" + e.Provider + ") " + e.Op + "(): " + e.Err
}

// Resources struct contains provider-specific details of every resource that
// we created, in a format understood by TearDown(), so that we can delete
// those resources when they're no longer needed. There are also fields for
// important things the user needs to know.
type Resources struct {
	ResourceName string             // the resource name prefix that resources were created with
	Details      map[string]string  // whatever key values the provider needs to describe what it created
	PrivateKey   string             // PEM format string of the key user would need to ssh in to any created servers
	Servers      map[string]*Server // the serverID => *Server mapping of any servers Spawn()ed with an external ip
}

// Quota struct describes the limit on what resources you are allowed to use (0
// values mean that resource is unlimited), and how much you have already used.
type Quota struct {
	MaxRAM        int // total MBs allowed
	MaxCores      int // total CPU cores allowed
	MaxInstances  int // max number of instances allowed
	MaxVolume     int // max GBs of volume storage that can be allocated
	UsedRAM       int
	UsedCores     int
	UsedInstances int
	UsedVolume    int
}

// provideri must be satisfied to add support for a particular cloud provider.
type provideri interface {
	// return the environment variables required to function
	requiredEnv() []string
	// return the environment variables that might be required to function
	maybeEnv() []string
	// do any initial config set up such as authentication
	initialize() error
	// achieve the aims of Deploy(), recording what you create in resources.Details and resources.PrivateKey
	deploy(ctx context.Context, resources *Resources, requiredPorts []int, useConfigDrive bool, gatewayIP,
		cidr string, dnsNameServers []string) error
	// achieve the aims of InCloud()
	inCloud(ctx context.Context) bool
	// return the details of any existing servers, as a slice of the details
	// returned by spawn
	getCurrentServers(resources *Resources) ([][]string, error)
	// achieve the aims of GetQuota()
	getQuota(ctx context.Context) (*Quota, error)
	// return a map of all server flavors, with their flavor ids as keys
	flavors(ctx context.Context) map[string]*Flavor
	// achieve the aims of Spawn(). Must send on the supplied usingQuotaCh as
	// soon as the new server has been requested and is counted as using up
	// quota (or the request fails), then create sentinelFilePath once the new
	// server is in powered up (but not necessarily fully booted up).
	spawn(ctx context.Context, resources *Resources, os string, flavor string, diskGB int, externalIP bool,
		usingQuotaCh chan bool) (serverID, serverIP, serverName, adminPass string, err error)
	// achieve the aims of ErrIsNoHardware()
	errIsNoHardware(err error) bool
	// achieve the aims of CheckServer()
	checkServer(serverID string) (bool, error)
	// achieve the aims of serverIsKnown()
	serverIsKnown(serverID string) (bool, error)
	// achieve the aims of DestroyServer()
	destroyServer(ctx context.Context, serverID string) error
	// achieve the aims of TearDown()
	tearDown(ctx context.Context, resources *Resources) error
}

// Provider gives you access to all of the methods you'll need to interact with
// a cloud provider.
type Provider struct {
	impl         provideri
	Name         string
	savePath     string
	resources    *Resources
	inCloud      bool
	madeHeadNode bool
	servers      map[string]*Server // by name
	sync.RWMutex
}

// DeployConfig are the configuration options that you supply to Deploy().
type DeployConfig struct {
	// RequiredPorts is the slice of port numbers that your application needs to
	// be able to communicate to any servers you spawn (eg. [22] for ssh)
	// through. This will typically translate to the creation of security groups
	// that open up these ports. If your cloud network has all ports open and
	// does not allow the application of security groups to created servers,
	// then provide an empty slice.
	RequiredPorts []int

	// UseConfigDrive, if set to true (default false), will cause all newly
	// spawned servers to mount a configuration drive, which is typically needed
	// for a network without DHCP.
	UseConfigDrive bool

	// CIDR is used to either determine which existing network (that the current
	// cloud host is attached to) to spawn new servers on, or to define the
	// properties of the network and subnet if those need to be created. CIDR
	// defaults to 192.168.64.0/18, allowing for 16384 servers to be Spawn()d
	// later, with a maximum ip of 192.168.127.255.
	CIDR string

	// GatewayIP is used if a network and subnet needed to be created, and is
	// the gateway IP address of the created subnet. It defaults to
	// 192.168.64.1.
	GatewayIP string

	// DNSNameServers is a slice of DNS name server IPs. It defaults to
	// Google's: []string{"8.8.4.4", "8.8.8.8"}.
	DNSNameServers []string
}

// RequiredEnv returns the environment variables that are needed by the given
// provider before New() will work for it. See New() for possible providerNames.
func RequiredEnv(providerName string) ([]string, error) {
	var p *Provider
	switch providerName {
	case openstackName:
		p = &Provider{impl: new(openstackp)}
	default:
		return nil, Error{providerName, "RequiredEnv", ErrBadProvider}
	}
	return p.impl.requiredEnv(), nil
}

// MaybeEnv returns the environment variables that may be needed by the given
// provider. If one of these is actually needed but not provided, errors may
// appear from New() or later in subsequent attempted method usage.
func MaybeEnv(providerName string) ([]string, error) {
	var p *Provider
	switch providerName {
	case openstackName:
		p = &Provider{impl: new(openstackp)}
	default:
		return nil, Error{providerName, "MaybeEnv", ErrBadProvider}
	}
	return p.impl.maybeEnv(), nil
}

// AllEnv returns the environment variables that RequiredEnv() and MaybeEnv()
// would return.
func AllEnv(providerName string) ([]string, error) {
	var p *Provider
	switch providerName {
	case openstackName:
		p = &Provider{impl: new(openstackp)}
	default:
		return nil, Error{providerName, "MaybeEnv", ErrBadProvider}
	}

	var all []string
	all = append(all, p.impl.requiredEnv()...)
	all = append(all, p.impl.maybeEnv()...)
	return all, nil
}

// New creates a new Provider to interact with the given cloud provider.
// Possible names so far are "openstack" ("aws" is planned). You must provide a
// resource name that will be used to name any created cloud resources. You must
// also provide a file path prefix to save details of created resources to (the
// actual file created will be suffixed with your resourceName).
//
// Note that the file could contain created private key details, so should be
// kept accessible only by you.
//
// Providing a logger allows for debug messages to be logged somewhere, along
// with any "harmless" or unreturnable errors. If not supplied, we use a default
// logger that discards all log messages.
func New(ctx context.Context, name string, resourceName string, savePath string) (*Provider, error) {
	var p *Provider
	switch name {
	case openstackName:
		p = &Provider{impl: new(openstackp)}
	default:
		return nil, Error{name, "New", ErrBadProvider}
	}

	p.Name = name
	p.savePath = savePath + "." + resourceName

	// var l log15.Logger
	// if len(logger) == 1 {
	// 	l = logger[0].New()
	// } else {
	// 	l = log15.New()
	// 	l.SetHandler(log15.DiscardHandler())
	// }
	// p.Logger = l

	// load any resources we previously saved, or get an empty set to work
	// with
	var err error
	p.resources, err = p.loadResources(ctx, resourceName)
	if err != nil {
		return nil, err
	}

	p.servers = make(map[string]*Server)
	for _, server := range p.resources.Servers {
		p.servers[nameToHostName(server.Name)] = server
	}

	var missingEnv []string
	for _, envKey := range p.impl.requiredEnv() {
		if os.Getenv(envKey) == "" {
			missingEnv = append(missingEnv, envKey)
		}
	}
	if len(missingEnv) > 0 {
		return nil, Error{name, "New", ErrMissingEnv + strings.Join(missingEnv, ", ")}
	}

	err = p.impl.initialize()
	if err != nil {
		return nil, err
	}

	p.inCloud = p.impl.inCloud(ctx)
	return p, nil
}

// Deploy triggers the creation of required cloud resources such as networks,
// ssh keys, security profiles and so on, such that you can subsequently Spawn()
// and ssh to your created server successfully.
//
// If a resource we need already exists with the resourceName you supplied to
// New(), we assume it belongs to us and we don't create another (so it is safe
// to call Deploy multiple times with the same args to New() and Deploy(): you
// don't need to check if you have already deployed).
//
// Deploy() saves the resources it created to disk, which are
// what TearDown() will delete when you call it. (They are saved to disk so that
// TearDown() can work if you call it in a different session to when you
// Deploy()ed, and so that PrivateKey() can work if you call it in a different
// session to the Deploy() call that actually created the ssh key.)
//
// If servers already seem to exist with a name prefix matching resourceName, we
// assume that they are from a prior deployment that crashed and you are now
// recovering the sitution; these servers can be retrieved with
// GetServerByName() and Destroy()ed. Note, however, that they aren't fully
// useable since we don't know the username needed to ssh to them.
func (p *Provider) Deploy(ctx context.Context, config *DeployConfig) error {
	gatewayIP := config.GatewayIP
	if gatewayIP == "" {
		gatewayIP = defaultGateWayIP
	}
	cidr := config.CIDR
	if cidr == "" {
		cidr = defaultCIDR
	}
	dnsNameServers := config.DNSNameServers
	if dnsNameServers == nil {
		dnsNameServers = defaultDNSNameServers[:]
	}

	// impl.deploy should overwrite any existing values in p.resources with
	// updated values, but should leave other things - such as an existing
	// PrivateKey when we have not just made a new one - alone
	err := p.impl.deploy(ctx, p.resources, config.RequiredPorts, config.UseConfigDrive, gatewayIP, cidr, dnsNameServers)
	if err != nil {
		return err
	}

	// save updated resources to disk
	err = p.saveResources(ctx)
	if err != nil {
		return err
	}

	privateKey := p.PrivateKey()
	p.Lock()
	defer p.Unlock()
	for _, server := range p.resources.Servers {
		if server.IsHeadNode {
			known, errk := p.ServerIsKnown(server.ID)
			if errk != nil {
				return errk
			}
			if known {
				p.madeHeadNode = true

				break
			}
		}
	}

	// record any existing servers that we may have spawned previously, then we
	// crashed, and now we're recovering. These server objects cannot be used
	// fully; you can't ssh without setting the UserName first. Intended just to
	// terminate
	sdetails, err := p.impl.getCurrentServers(p.resources)
	if err != nil {
		return err
	}

	for _, details := range sdetails {
		p.servers[nameToHostName(details[2])] = &Server{
			ID:           details[0],
			Name:         details[2],
			IP:           details[1],
			AdminPass:    details[3],
			PrivateKey:   privateKey,
			provider:     p,
			cancelRunCmd: make(map[int]chan bool),
			created:      false,
		}
	}

	return err
}

// InCloud tells you if your process is currently running on a cloud server
// where the *Server related methods will all work correctly. (That is, if this
// returns true, you are on the same network as any server you Spawn().)
func (p *Provider) InCloud() bool {
	return p.inCloud
}

// GetQuota returns details of the maximum resources the user can request, and
// the current resources used.
func (p *Provider) GetQuota(ctx context.Context) (*Quota, error) {
	return p.impl.getQuota(ctx)
}

// CheapestServerFlavor returns details of the smallest (cheapest) server
// "flavor" available that satisfies your minimum ram (MB) and CPU (core count)
// requirements, and that also matches the given regex (empty string for the
// regex means not limited by regex). Use the ID property of the return value
// for passing to Spawn(). If no flavor meets your requirements you will get an
// error matching ErrNoFlavor. You don't test for size of disk here, because
// during Spawn() you will request a certain amount of disk space, and if that
// is larger than the flavor's root disk a larger volume will be created
// automatically.
func (p *Provider) CheapestServerFlavor(ctx context.Context, cores, ramMB int, regex string) (*Flavor, error) {
	r, err := p.regexStrToRegexp(regex)
	if err != nil {
		return nil, err
	}

	f := p.pickCheapestFlavorFromSubset(ctx, cores, ramMB, r, []*regexp.Regexp{})
	if f == nil {
		return nil, Error{"cloud", "CheapestServerFlavor", ErrNoFlavor}
	}
	return f, nil
}

// regexStrToRegexp converts a regex string to a Regexp. If regex is a blank
// string, returns nil and no error.
func (p *Provider) regexStrToRegexp(regex string) (*regexp.Regexp, error) {
	if regex == "" {
		return nil, nil
	}
	r, err := regexp.Compile(regex)
	if err != nil {
		return nil, Error{"cloud", "CheapestServerFlavor", ErrBadRegex}
	}
	return r, nil
}

// pickCheapestFlavorFromSubset looks through p.impl.flavors() for the
// cheapest flavor with a Name that matches the regexp, and that also matches
// at least one of the regexps in the subset. regexp can be nil to match any
// flavor, and subset can be empty to pick from the superset, but subset
// elements cannot be nil.
func (p *Provider) pickCheapestFlavorFromSubset(ctx context.Context, cores, ramMB int, regexp *regexp.Regexp, subset []*regexp.Regexp) *Flavor {
	// from flavours in the subset, pick the one that has the lowest ram, disk
	// and cpus that meet our minimums, and also matches the regex
	var fr *Flavor

	for _, f := range p.impl.flavors(ctx) {
		if regexp != nil && !regexp.MatchString(f.Name) {
			continue
		}

		var inSubset bool
		if len(subset) == 0 {
			inSubset = true
		} else {
			for _, r := range subset {
				if r.MatchString(f.Name) {
					inSubset = true
					break
				}
			}
		}
		if !inSubset {
			continue
		}

		if f.Cores >= cores && f.RAM >= ramMB {
			if fr != nil {
				if f.Cores < fr.Cores {
					fr = f
				} else if f.Cores == fr.Cores {
					if f.RAM < fr.RAM {
						fr = f
					} else if f.RAM == fr.RAM && f.Disk < fr.Disk {
						fr = f
					}
				}
			} else {
				fr = f
			}
		}
	}
	return fr
}

// CheapestServerFlavors is like CheapestServerFlavor(), taking the same first 3
// arguments, but also a slice of slices that describe sets of flavors. For
// example, [][]string{{"f1","f2"},{"f3","f4"}}. Here, flavors f1 and f2 are in
// one set, and f3 and f4 are in another. The names are treated as regular
// expressions so you can describe multiple flavors in a set with a single
// entry.
//
// You will get back the cheapest server flavor in each set, in the order of the
// sets you supply. The length of the returned slice will match length of sets.
// If, say, the 3rd set does not have a suitable server at all, then the 3rd
// element of the returned slice will be nil.
//
// In the special case that sets is an empty slice, returns the result of
// CheapestServerFlavor() in a 1 element slice.
func (p *Provider) CheapestServerFlavors(ctx context.Context, cores, ramMB int,
	regex string, sets [][]string) ([]*Flavor, error) {
	if len(sets) == 0 {
		f, err := p.CheapestServerFlavor(ctx, cores, ramMB, regex)
		return []*Flavor{f}, err
	}

	// (because flavors can change over time, we can't cache the result of this
	// calculation and must run it every time)
	r, err := p.regexStrToRegexp(regex)
	if err != nil {
		return nil, err
	}

	matches := make([]*Flavor, len(sets))

	for i, set := range sets {
		subset := make([]*regexp.Regexp, len(set))
		for j, flavor := range set {
			rf, err := p.regexStrToRegexp(flavor)
			if err != nil {
				return nil, err
			}
			subset[j] = rf
		}

		matches[i] = p.pickCheapestFlavorFromSubset(ctx, cores, ramMB, r, subset)
	}

	return matches, nil
}

// GetServerFlavor returns the flavor with the given ID or name. If no flavor
// exactly matches you will get an error matching ErrBadFlavor.
func (p *Provider) GetServerFlavor(ctx context.Context, idOrName string) (*Flavor, error) {
	flavors := p.impl.flavors(ctx)
	fr, existed := flavors[idOrName]

	if !existed {
		for _, f := range flavors {
			if f.Name == idOrName {
				fr = f
				break
			}
		}
	}

	if fr == nil {
		return nil, Error{"cloud", "GetServerFlavor", ErrBadFlavor}
	}

	return fr, nil
}

// SpawnUsingQuotaCallback is the callback function you supply to Spawn() that
// will be called as soon as the request for the new server has been issued and
// is counted as using up quota (but before it has powered up).
type SpawnUsingQuotaCallback func()

// Spawn creates a new server using an OS image with a name or ID prefixed with
// the given os name or ID, with the given flavor ID (that you could get from
// CheapestServerFlavor().ID) and at least the given amount of disk space
// (creating a temporary volume of the required size if the flavor's root disk
// is too small).
//
// If you supply a non-zero value for the ttd argument, then this amount of time
// after the last s.Release() call you make (or after the last cmd you started
// with s.RunCmd exits) that causes the server to be considered idle, the server
// will be destroyed.
//
// If you need an external IP so that you can ssh to the server externally,
// supply true for externalIP.
//
// You can supply an optinoal callback function that will be called as soon as
// the new server request has gone out (and so is using up your quota), but
// before the new server has powered up (which is when Spawn() will return the
// new server details).
//
// Returns a *Server so you can s.Destroy it later, find out its ip address so
// you can ssh to it, and get its admin password in case you need to sudo on the
// server. You will need to know the username that you can log in with on your
// chosen OS image. If you call Spawn() while running on a cloud server, then
// the newly spawned server will be in the same network and security group as
// the current server. If you get an err, you will want to call server.Destroy()
// as this is not done for you.
//
// NB: the server will likely not be ready to use yet, having not completed its
// boot up; call server.WaitUntilReady() before trying to use the server for
// anything.
func (p *Provider) Spawn(ctx context.Context, os string, osUser string, flavorID string,
	diskGB int, ttd time.Duration, externalIP bool, usingQuotaCB ...SpawnUsingQuotaCallback) (*Server, error) {
	f, found := p.impl.flavors(ctx)[flavorID]
	if !found {
		return nil, Error{"cloud", "Spawn", ErrBadFlavor}
	}

	usingQuota := make(chan bool)
	go func() {
		<-usingQuota
		close(usingQuota)
		if len(usingQuotaCB) == 1 {
			usingQuotaCB[0]()
		}
	}()

	serverID, serverIP, serverName, adminPass, err := p.impl.spawn(ctx, p.resources, os,
		flavorID, diskGB, externalIP, usingQuota)

	if err != nil {
		return nil, err
	}

	maxDisk := f.Disk
	if diskGB > maxDisk {
		maxDisk = diskGB
	}

	server := &Server{
		ID:           serverID,
		Name:         serverName,
		IP:           serverIP,
		OS:           os,
		AdminPass:    adminPass,
		PrivateKey:   p.PrivateKey(),
		UserName:     osUser,
		Flavor:       f,
		Disk:         maxDisk,
		TTD:          ttd,
		provider:     p,
		cancelRunCmd: make(map[int]chan bool),
		created:      true,
	}

	p.Lock()
	p.servers[nameToHostName(serverName)] = server

	if err == nil && externalIP {
		// if this is the first server created, note it is the "head node"
		if !p.madeHeadNode {
			server.IsHeadNode = true
			p.madeHeadNode = true
		}

		// update resources and save to disk
		p.resources.Servers[serverID] = server
		p.Unlock()
		err = p.saveResources(ctx)
	} else {
		p.Unlock()
	}

	return server, err
}

// ErrIsNoHardware return true if the given error suggests failure to spawn a
// server due to lack of hardware.
func (p *Provider) ErrIsNoHardware(err error) bool {
	return p.impl.errIsNoHardware(err)
}

// CheckServer asks the provider if the status of the given server (id retrieved
// via Spawn() or Servers()) indicates it is working fine. (If it's not and
// was previously thought to be a spawned server with an external IP, then it
// will be removed from the results of Servers().)
func (p *Provider) CheckServer(ctx context.Context, serverID string) (working bool, err error) {
	working, err = p.impl.checkServer(serverID)

	if err == nil && !working {
		// update resources and save to disk
		p.Lock()
		if _, present := p.resources.Servers[serverID]; present {
			delete(p.resources.Servers, serverID)
			p.Unlock()
			err = p.saveResources(ctx)
		} else {
			p.Unlock()
		}
	}

	return working, err
}

// ServerIsKnown asks the provider if the given server (id retrieved via Spawn()
// or Servers()) is known about by this provider. If this returns false, it
// doesn't necessarily mean the server doesn't exist; it could mean you supplied
// the wrong credentials for the resources file you passed to New(). For that
// reason, the resources file is not updated if this returns false, and
// Servers() will continue to return a server with the given serverID.
func (p *Provider) ServerIsKnown(serverID string) (known bool, err error) {
	return p.impl.serverIsKnown(serverID)
}

// DestroyServer destroys a server given its id, that you would have gotten from
// the ID property of Spawn()'s return value.
func (p *Provider) DestroyServer(ctx context.Context, serverID string) error {
	err := p.impl.destroyServer(ctx, serverID)
	if err != nil {
		return err
	}

	// update resources and save to disk
	p.Lock()
	delete(p.resources.Servers, serverID)
	p.Unlock()

	return p.saveResources(ctx)
}

// Servers returns a mapping of serverID => *Server for all servers that were
// Spawn()ed with an external IP (including those spawned in past sessions where
// the same arguments to New() were used). You should use s.Alive() before
// trying to use one of these servers. Do not alter the return value!
func (p *Provider) Servers() map[string]*Server {
	p.RLock()
	defer p.RUnlock()
	return p.resources.Servers
}

// GetServerByName returns the Server with the given hostname. Returns nil if we
// did not spawn a server with that hostname.
func (p *Provider) GetServerByName(name string) *Server {
	p.RLock()
	defer p.RUnlock()
	return p.servers[name]
}

// HeadNode returns the first server created under this deployment that had an
// external IP. Returns nil if no such server was recorded.
func (p *Provider) HeadNode() *Server {
	p.RLock()
	defer p.RUnlock()
	for _, server := range p.resources.Servers {
		if server.IsHeadNode {
			return server
		}
	}
	return nil
}

// LocalhostServer returns a Server object with details of the host we are
// currently running on. No cloud API calls are made to construct this.
func (p *Provider) LocalhostServer(os string, postCreationScript []byte, configFiles string, cidr ...string) (*Server, error) {
	maxRAM, err := internal.ProcMeminfoMBs()
	if err != nil {
		return nil, err
	}

	diskSize := local.NewVolume(".").Size(context.Background())

	ip, err := internal.CurrentIP(cidr[0])
	if err != nil {
		return nil, err
	}

	user, err := internal.Username()
	if err != nil {
		return nil, err
	}

	return &Server{
		Name:        "localhost",
		IP:          ip,
		OS:          os,
		UserName:    user,
		Script:      postCreationScript,
		ConfigFiles: configFiles,
		Flavor: &Flavor{
			RAM:   maxRAM,
			Cores: runtime.NumCPU(),
			Disk:  diskSize,
		},
		Disk:         diskSize,
		provider:     p,
		cancelRunCmd: make(map[int]chan bool),
	}, nil
}

// PrivateKey returns a PEM format string of the private key that was created
// by Deploy() (on its first invocation with the same arguments to New()).
func (p *Provider) PrivateKey() string {
	p.RLock()
	defer p.RUnlock()
	return p.resources.PrivateKey
}

// TearDown deletes all resources recorded during Deploy() or loaded from a
// previous session during New(). It also deletes any servers with names
// prefixed with the resourceName given to the initial New() call. If currently
// running on a cloud server, however, it will not delete anything needed by
// this server, including the resource file that contains the private key.
func (p *Provider) TearDown(ctx context.Context) error {
	p.RLock()
	defer p.RUnlock()
	err := p.impl.tearDown(ctx, p.resources)
	if err != nil {
		return err
	}

	// delete our savePath unless our resources still contains the private key,
	// indicating it is still in the cloud and could be needed in the future
	if p.resources.PrivateKey == "" {
		err = p.deleteResourceFile()
		if os.IsNotExist(err) {
			err = nil
		}
	}
	return err
}

// saveResources saves our resources to our savePath, overwriting any existing
// content.
func (p *Provider) saveResources(ctx context.Context) error {
	p.Lock()
	defer p.Unlock()
	file, err := os.OpenFile(p.savePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	defer internal.LogClose(ctx, file, "resource file", "path", p.savePath)

	encoder := gob.NewEncoder(file)
	return encoder.Encode(p.resources)
}

// loadResources loads our resources from our savePath, or returns an empty
// set of resources if savePath doesn't exist.
func (p *Provider) loadResources(ctx context.Context, resourceName string) (*Resources, error) {
	resources := &Resources{ResourceName: resourceName, Details: make(map[string]string), Servers: make(map[string]*Server)}
	if _, serr := os.Stat(p.savePath); os.IsNotExist(serr) {
		return resources, nil
	}

	file, err := os.Open(p.savePath)
	if err != nil {
		return nil, err
	}

	defer internal.LogClose(ctx, file, "resource file", "path", p.savePath)

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(resources)
	if err != nil {
		return nil, err
	}

	// add in the ref to ourselves to each of our servers
	for _, server := range resources.Servers {
		server.provider = p
		server.cancelRunCmd = make(map[int]chan bool)
	}
	return resources, nil
}

// deleteResourceFile deletes our savePath.
func (p *Provider) deleteResourceFile() error {
	return os.Remove(p.savePath)
}

// uniqueResourceName takes the given prefix and appends a unique string to it
// (a uuid).
func uniqueResourceName(prefix string) string {
	u, _ := uuid.NewV4() // this used to return no error, and now I don't want to change my own method signature...
	return prefix + "-" + u.String()
}

// nameToHostName makes the given name compatible with being a hostname in the
// same way that OpenStack horizon does: convert to lower case and convert non
// [a-z1-9\-] characters to - characters. Also truncates to 63 characters.
func nameToHostName(name string) string {
	hostname := strings.ToLower(name)
	hostname = hostNameRegex.ReplaceAllString(hostname, "-")
	if len(hostname) > 63 {
		hostname = hostname[0:63]
	}
	return hostname
}
