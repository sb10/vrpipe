// Copyright © 2016-2021 Genome Research Limited
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

package scheduler

// This file contains a scheduleri implementation for 'openstack': running jobs
// on servers spawned on demand.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"time"

	sync "github.com/sasha-s/go-deadlock"
	"github.com/wtsi-ssg/wr/clog"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/queue"
	"github.com/patrickmn/go-cache"
)

const (
	unquotadVal                  = 1000000 // a "large" number for use when we don't have quota
	serverNotNeededErrStr        = "server not needed"
	localhostName                = "localhost"
	flavorFailedCacheExpiry      = 15 * time.Minute
	flavorFailedCacheCleanup     = 30 * time.Minute
	flavorDeterminedCacheExpiry  = 5 * time.Minute
	flavorDeterminedCacheCleanup = 10 * time.Minute
)

// debugCounter and debugEffect are used by tests to prove some bugs
var debugCounter int
var debugEffect string

// opst is our implementer of scheduleri. It takes much of its implementation
// from the local scheduler.
type opst struct {
	local
	flavorSets        [][]string
	config            *ConfigOpenStack
	provider          *cloud.Provider
	quotaMaxInstances int
	quotaMaxCores     int
	quotaMaxRAM       int
	quotaMaxVolume    int
	reservedInstances int
	reservedCores     int
	reservedRAM       int
	reservedVolume    int
	spawningNow       map[string]int
	servers           map[string]*cloud.Server
	spawnedServers    map[string]*cloud.Server
	msgCB             MessageCallBack
	badServerCB       BadServerCallBack
	recoveredServers  map[string]bool
	stopRSMonitoring  chan struct{}
	ffCache           *cache.Cache
	dfCache           *cache.Cache
	serversMutex      sync.RWMutex
	cbmutex           sync.RWMutex
	scMutex           sync.Mutex
	stateMutex        sync.Mutex
	rsMutex           sync.Mutex
	spawnMutex        sync.Mutex
	spawnCanceller    map[string]map[string]chan struct{}
	updatingState     bool
}

// ConfigOpenStack represents the configuration options required by the
// OpenStack scheduler. All are required with no usable defaults, unless
// otherwise noted. This struct implements the CloudConfig interface.
type ConfigOpenStack struct {
	// ResourceName is the resource name prefix used to name any resources (such
	// as keys, security groups and servers) that need to be created.
	ResourceName string

	// OSPrefix is the prefix or full name of the Operating System image you
	// wish spawned servers to run by default (overridden during Schedule() by a
	// Requirements.Other["cloud_os"] value)
	OSPrefix string

	// OSUser is the login username of your chosen Operating System from
	// OSPrefix. (Overridden during Schedule() by a
	// Requirements.Other["cloud_user"] value.)
	OSUser string

	// OSRAM is the minimum RAM in MB needed to bring up a server instance that
	// runs your Operating System image. It defaults to 2048. (Overridden during
	// Schedule() by a Requirements.Other["cloud_os_ram"] value.)
	OSRAM int

	// OSDisk is the minimum disk in GB with which to bring up a server instance
	// that runs your Operating System image. It defaults to 1. (Overridden
	// during Schedule() by a Requirements.Disk value.)
	OSDisk int

	// FlavorRegex is a regular expression that you can use to limit what
	// flavors of server will be created to run commands on. The default of an
	// empty string means there is no limit, and any available flavor can be
	// used. (The flavor chosen for a command will be the flavor with the least
	// specifications (RAM, CPUs, Disk) capable of running the command, that
	// also satisfies this regex.)
	FlavorRegex string

	// FlavorSets is used to describe sets of flavors that will only run on
	// certain subsets of your available hardware. If a flavor in set 1 is
	// chosen, but OpenStack reports it isn't possible to create a server with
	// that flavor because there is no more available hardware to back it, then
	// the next best flavor in a different flavor set will be attempted. The
	// value here is a string in the form f1,f2;f3,f4 where f1 and f2 are in the
	// same set, and f3 and f4 are in a different set. The names of each flavor
	// are treates as regular expressions, so you may be able to describe all
	// the flavors in a set with a single entry.
	FlavorSets string

	// PostCreationScript is the []byte content of a script you want executed
	// after a server is Spawn()ed. (Overridden during Schedule() by a
	// Requirements.Other["cloud_script"] value.)
	PostCreationScript []byte

	// ConfigFiles is a comma separated list of paths to config files that
	// should be copied over to all spawned servers. Absolute paths are copied
	// over to the same absolute path on the new server. To handle a config file
	// that should remain relative to the home directory (and where the spawned
	// server may have a different username and thus home directory path
	// compared to the current server), use the prefix ~/ to signify the home
	// directory. It silently ignores files that don't exist locally.
	// (Appended to during Schedule() by a
	// Requirements.Other["cloud_config_files"] value.)
	ConfigFiles string

	// SavePath is an absolute path to a file on disk where details of any
	// created resources can be read from and written to.
	SavePath string

	// ServerKeepTime is the time to wait before an idle server is destroyed.
	// Zero duration means "never destroy due to being idle".
	ServerKeepTime time.Duration

	// StateUpdateFrequency is the frequency at which to check spawned servers
	// that are being used to run things, to see if they're still alive.
	// 0 (default) is treated as 1 minute.
	StateUpdateFrequency time.Duration

	// MaxInstances is the maximum number of instances we are allowed to spawn.
	// -1 means we will be limited by your quota, if any. 0 (the default) means
	// no additional instances will be spawned (commands will run locally on the
	// same instance the manager is running on).
	MaxInstances int

	// SimultaneousSpawns is the maximum number of instances we are allowed to
	// try and spawn simultaneously. 0 (the default) means unlimited. 1 would
	// mean all spawns occur sequentially, which may be more reliable, but would
	// result in very slow scale up.
	SimultaneousSpawns int

	// MaxLocalCores is the maximum number of cores that can be used to run
	// commands on the same instance the manager is running on. -1 (the default)
	// means all cores can be used. 0 will only allow 0 core cmds to run on it.
	// To distinguish "not defined" from 0, the value is a reference to an int.
	MaxLocalCores *int

	// MaxLocalRAM is the maximum number of MB of memory that can be used to run
	// commands on the same instance the manager is running on. -1 (the default)
	// means all memory can be used. 0 disables running commands on the
	// manager's instance. To distinguish "not defined" from 0, the value is a
	// reference to an int.
	MaxLocalRAM *int

	// Shell is the shell to use to run your commands with; 'bash' is
	// recommended.
	Shell string

	// ServerPorts are the TCP port numbers you need to be open for
	// communication with any spawned servers. At a minimum you will need to
	// specify []int{22}, unless the network you use has all ports open and does
	// not support applying security groups to servers, in which case you must
	// supply an empty slice.
	ServerPorts []int

	// UseConfigDrive, if set to true (default false), will cause all newly
	// spawned servers to mount a configuration drive, which is typically needed
	// for a network without DHCP.
	UseConfigDrive bool

	// CIDR describes the range of network ips that can be used to spawn
	// OpenStack servers on which to run our commands. The default is
	// "192.168.64.0/18", which allows for 16384 servers to be spawned. This
	// range ends at 192.168.127.255. If already in OpenStack, this chooses
	// which existing network (that the current host is attached to) to use.
	// Otherwise, this results in the creation of an appropriately configured
	// network and subnet.
	CIDR string

	// GatewayIP is the gateway ip address for the subnet that will be created
	// with the given CIDR. It defaults to 192.168.64.1.
	GatewayIP string

	// DNSNameServers is a slice of DNS IP addresses to use for lookups on the
	// created subnet. It defaults to Google's: []string{"8.8.4.4", "8.8.8.8"}.
	DNSNameServers []string

	// Umask is an optional umask to run remote commands under, to control the
	// permissions of files created on spawned OpenStack servers. If not
	// supplied (0), the umask used will be the default umask of the OSUser
	// user. Note that setting this will result in scheduled commands being
	// executed like `(umask Umask && cmd)`, which may present cross-platform
	// compatibility issues. (But should work on most linux-like systems.)
	Umask int
}

// AddConfigFile takes a value as per the ConfigFiles property, and appends it
// to the existing ConfigFiles value (or sets it if unset).
func (c *ConfigOpenStack) AddConfigFile(configFile string) {
	if c.ConfigFiles == "" {
		c.ConfigFiles = configFile
	} else {
		c.ConfigFiles += "," + configFile
	}
}

// GetOSUser returns OSUser, to meet the CloudConfig interface.
func (c *ConfigOpenStack) GetOSUser() string {
	return c.OSUser
}

// GetServerKeepTime returns ServerKeepTime, to meet the CloudConfig interface.
func (c *ConfigOpenStack) GetServerKeepTime() time.Duration {
	return c.ServerKeepTime
}

// initialize sets up an openstack scheduler.
func (s *opst) initialize(ctx context.Context, config interface{}) error {
	s.config = config.(*ConfigOpenStack)
	if s.config.OSRAM == 0 {
		s.config.OSRAM = 2048
	}
	if s.config.OSDisk == 0 {
		s.config.OSDisk = 1
	}

	// create a cloud provider for openstack, that we'll use to interact with
	// openstack
	provider, err := cloud.New(ctx, "openstack", s.config.ResourceName, s.config.SavePath)
	if err != nil {
		return err
	}
	s.provider = provider

	err = provider.Deploy(ctx, &cloud.DeployConfig{
		RequiredPorts:  s.config.ServerPorts,
		UseConfigDrive: s.config.UseConfigDrive,
		GatewayIP:      s.config.GatewayIP,
		CIDR:           s.config.CIDR,
		DNSNameServers: s.config.DNSNameServers,
	})
	if err != nil {
		return err
	}

	// to debug spawned servers that don't work correctly:
	// keyFile := filepath.Join("/tmp", "key")
	// os.WriteFile(keyFile, []byte(provider.PrivateKey()), 0600)

	// query our quota maximums for cpu and memory and total number of
	// instances; 0 will mean unlimited
	quota, err := provider.GetQuota(ctx)
	if err != nil {
		return err
	}
	if quota.MaxCores == 0 {
		s.quotaMaxCores = unquotadVal
	} else {
		s.quotaMaxCores = quota.MaxCores
	}
	if quota.MaxRAM == 0 {
		s.quotaMaxRAM = unquotadVal
	} else {
		s.quotaMaxRAM = quota.MaxRAM
	}
	if quota.MaxVolume == 0 {
		s.quotaMaxVolume = unquotadVal
	} else {
		s.quotaMaxVolume = quota.MaxVolume
	}
	if quota.MaxInstances == 0 {
		s.quotaMaxInstances = unquotadVal
	} else {
		s.quotaMaxInstances = quota.MaxInstances
	}
	if s.config.MaxInstances > -1 && s.config.MaxInstances < s.quotaMaxInstances {
		s.quotaMaxInstances = s.config.MaxInstances
		if provider.InCloud() {
			s.quotaMaxInstances++
		}
	}

	// initialize our job queue and other trackers
	s.queue = queue.New(ctx, localPlace)
	s.running = make(map[string]int)
	s.spawningNow = make(map[string]int)

	// initialise our servers with details of ourself
	s.servers = make(map[string]*cloud.Server)
	localhost, err := provider.LocalhostServer(s.config.OSPrefix, s.config.PostCreationScript, s.config.ConfigFiles, s.config.CIDR)
	if err != nil {
		return err
	}
	if s.config.MaxLocalCores != nil {
		if *s.config.MaxLocalCores >= 0 && *s.config.MaxLocalCores < localhost.Flavor.Cores {
			localhost.Flavor.Cores = *s.config.MaxLocalCores
		}
	}
	if s.config.MaxLocalRAM != nil {
		if *s.config.MaxLocalRAM >= 0 && *s.config.MaxLocalRAM < localhost.Flavor.RAM {
			localhost.Flavor.RAM = *s.config.MaxLocalRAM
		}
	}
	s.servers[localhostName] = localhost

	// set our functions for use in schedule() and processQueue()
	s.reqCheckFunc = s.reqCheck
	s.maxMemFunc = s.maxMem
	s.maxCPUFunc = s.maxCPU
	s.canCountFunc = s.canCount
	s.cantFunc = s.spawnMultiple
	s.runCmdFunc = s.runCmd
	s.stateUpdateFunc = s.stateUpdate
	s.stateUpdateFreq = s.config.StateUpdateFrequency
	if s.stateUpdateFreq == 0 {
		s.stateUpdateFreq = 1 * time.Minute
	}
	s.postProcessFunc = s.postProcess
	s.cmdNotNeededFunc = s.cmdNotNeeded
	s.spawnedServers = make(map[string]*cloud.Server)

	// pass through our shell config and logger to our local embed, as well as
	// creating its stopAuto channel
	s.local.config = &ConfigLocal{Shell: s.config.Shell}
	s.local.stopAuto = make(chan bool)

	s.recoveredServers = make(map[string]bool)
	s.stopRSMonitoring = make(chan struct{})
	s.spawnCanceller = make(map[string]map[string]chan struct{})

	if s.config.FlavorSets != "" {
		sets := strings.Split(s.config.FlavorSets, ";")
		for _, set := range sets {
			flavors := strings.Split(set, ",")
			s.flavorSets = append(s.flavorSets, flavors)
		}
	}

	s.ffCache = cache.New(flavorFailedCacheExpiry, flavorFailedCacheCleanup)
	s.dfCache = cache.New(flavorDeterminedCacheExpiry, flavorDeterminedCacheCleanup)

	return err
}

// reqCheck gives an ErrImpossible if the given Requirements can not be met,
// based on our quota and the available server flavours. Also based on the
// specific flavor the user has specified, if any.
func (s *opst) reqCheck(ctx context.Context, req *Requirements) error {
	reqForSpawn := s.reqForSpawn(req)

	// check if possible vs quota
	if reqForSpawn.RAM > s.quotaMaxRAM || int(math.Ceil(reqForSpawn.Cores)) > s.quotaMaxCores || reqForSpawn.Disk > s.quotaMaxVolume {
		clog.Warn(ctx, "Requested resources are greater than max quota", "quotaCores", s.quotaMaxCores, "requiredCores",
			reqForSpawn.Cores, "quotaRAM", s.quotaMaxRAM, "requiredRAM", reqForSpawn.RAM, "quotaDisk", s.quotaMaxVolume,
			"requiredDisk", reqForSpawn.Disk)
		s.notifyMessage(fmt.Sprintf("OpenStack: not enough quota for the job needing %f cores, %d RAM and %d Disk", reqForSpawn.Cores, reqForSpawn.RAM, reqForSpawn.Disk))
		return Error{"openstack", "schedule", ErrImpossible}
	}

	if name, defined := req.Other["cloud_flavor"]; defined {
		requestedFlavor, err := s.getFlavor(ctx, name)
		if err != nil {
			return err
		}

		// check that the user hasn't requested a flavor that isn't actually big
		// enough to run their job
		if requestedFlavor.Cores < int(math.Ceil(reqForSpawn.Cores)) || requestedFlavor.RAM < reqForSpawn.RAM {
			clog.Warn(ctx, "Requested flavor is too small for the job", "flavor", requestedFlavor.Name, "flavorCores",
				requestedFlavor.Cores, "requiredCores", reqForSpawn.Cores, "flavorRAM", requestedFlavor.RAM, "requiredRAM",
				reqForSpawn.RAM)
			s.notifyMessage(fmt.Sprintf("OpenStack: requested flavor %s is too small for the job needing %f cores and %d RAM", requestedFlavor.Name, reqForSpawn.Cores, reqForSpawn.RAM))
			return Error{"openstack", "schedule", ErrImpossible}
		}
	} else {
		// check if possible vs flavors
		_, err := s.determineFlavor(ctx, req, "")
		return err
	}
	return nil
}

// maxMem returns the maximum memory available in quota.
func (s *opst) maxMem() int {
	return s.quotaMaxRAM
}

// maxCPU returns the maximum number of CPU cores available quota.
func (s *opst) maxCPU() int {
	return s.quotaMaxCores
}

// determineFlavor picks a server flavor, preferring the smallest (cheapest)
// amongst those that are capable of running it from the earliest possible
// flavor set.
//
// If the initial pick is for a flavor that has been marked as unusable (because
// the last time we tried to spawn a server of the flavor it failed due to lack
// of hardware), we return the best pick from the next possible flavor set. If
// all possible picks from all flavor sets have been marked unusable, we return
// the flavor from the first possible flavor set, to give it another try.
//
// Since this is called during our canCount and then during runCmd for each
// "can", we want the return value to be the same for that set of calls, so we
// cache based on the "call" argument that processQueue sent in to canCount and
// runCmd, which in turn pass through to here.
func (s *opst) determineFlavor(ctx context.Context, req *Requirements, call string) (*cloud.Flavor, error) {
	if call != "" {
		if flavor, cached := s.dfCache.Get(call); cached {
			return flavor.(*cloud.Flavor), nil
		}
	}

	flavors, err := s.provider.CheapestServerFlavors(ctx, int(math.Ceil(req.Cores)), req.RAM,
		s.config.FlavorRegex, s.flavorSets)
	if err != nil {
		return nil, err
	}
	var hasFlavors bool
	for _, f := range flavors {
		if f != nil {
			hasFlavors = true
			break
		}
	}
	if !hasFlavors {
		err = Error{"openstack", "determineFlavor", ErrImpossible}
	} else if err != nil {
		if perr, ok := err.(cloud.Error); ok && perr.Err == cloud.ErrNoFlavor {
			err = Error{"openstack", "determineFlavor", ErrImpossible}
		}
	}
	if err != nil {
		return nil, err
	}

	var flavor *cloud.Flavor
	var pickedI int
	var pickedFirst bool
	for i, f := range flavors {
		if f == nil {
			continue
		}
		if flavor == nil {
			flavor = f
			pickedI = i
			pickedFirst = true
		}

		if _, failed := s.ffCache.Get(f.ID); failed {
			continue
		}

		flavor = f
		pickedI = i
		pickedFirst = false
		break
	}

	if pickedFirst {
		clog.Debug(ctx, "determineFlavor's picks were all failed, picking the one from the earliest flavor set",
			"set", pickedI, "flavor", flavor.Name)
	} else if pickedI != 0 {
		clog.Debug(ctx, "determineFlavor's first pick was failed, picking one that is unfailed",
			"set", pickedI, "flavor", flavor.Name)
	}

	if call != "" {
		s.dfCache.Set(call, flavor, cache.DefaultExpiration)
	}

	return flavor, err
}

// getFlavor returns a flavor with the given name or id. Returns an error
// if no matching flavor exists.
func (s *opst) getFlavor(ctx context.Context, name string) (*cloud.Flavor, error) {
	flavor, err := s.provider.GetServerFlavor(ctx, name)
	if err != nil {
		if perr, ok := err.(cloud.Error); ok && perr.Err == cloud.ErrNoFlavor {
			err = Error{"openstack", "getFlavorByName", ErrBadFlavor}
		}
	}
	return flavor, err
}

// serverReqs checks the given req's Other details to see if a particular kind
// of server has been requested. If not specified, the returned os defaults to
// the configured OSPrefix, script defaults to PostCreationScript, config files
// defaults to ConfigFiles and flavor will be nil.
func (s *opst) serverReqs(ctx context.Context, req *Requirements) (osPrefix string, osScript []byte,
	osConfigFiles string, flavor *cloud.Flavor, sharedDisk bool, err error) {
	if val, defined := req.Other["cloud_os"]; defined {
		osPrefix = val
	} else {
		osPrefix = s.config.OSPrefix
	}

	if val, defined := req.Other["cloud_script"]; defined {
		osScript = []byte(val)
	} else {
		osScript = s.config.PostCreationScript
	}

	if val, defined := req.Other["cloud_config_files"]; defined {
		if s.config.ConfigFiles != "" {
			osConfigFiles = s.config.ConfigFiles + "," + val
		} else {
			osConfigFiles = val
		}
	} else {
		osConfigFiles = s.config.ConfigFiles
	}

	if name, defined := req.Other["cloud_flavor"]; defined {
		flavor, err = s.getFlavor(ctx, name)
		if err != nil {
			return osPrefix, osScript, osConfigFiles, flavor, sharedDisk, err
		}
	}

	if val, defined := req.Other["cloud_shared"]; defined && val == "true" {
		sharedDisk = true

		// create a shared disk on our "head" node (if not already done)
		s.serversMutex.RLock()
		err = s.servers[localhostName].CreateSharedDisk()
		s.serversMutex.RUnlock()
	}

	return osPrefix, osScript, osConfigFiles, flavor, sharedDisk, err
}

// canCount tells you how many jobs with the given RAM and core requirements it
// is possible to run, given remaining resources in existing servers.
func (s *opst) canCount(ctx context.Context, cmd string, req *Requirements, call string) int {
	if s.cleanedUp() {
		return 0
	}

	requestedOS, requestedScript, requestedConfigFiles, requestedFlavor, needsSharedDisk, err := s.serverReqs(ctx, req)
	if err != nil {
		clog.Warn(ctx, "Failed to determine server requirements", "err", err)
		return 0
	}

	// we don't do any actual checking of current resources on the machines, but
	// instead rely on our simple tracking based on how many cores and RAM
	// prior cmds were /supposed/ to use. This could be bad for misbehaving cmds
	// that use too much memory, but we will end up killing cmds that do this,
	// so it shouldn't be too much of an issue.

	// see how many of these commands will run on existing servers
	var canCount int
	s.serversMutex.RLock()
	for _, server := range s.servers {
		if !server.IsBad() && server.Matches(requestedOS, requestedScript, requestedConfigFiles, requestedFlavor, needsSharedDisk) {
			space := server.HasSpaceFor(req.Cores, req.RAM, req.Disk)
			canCount += space
		}
	}
	s.serversMutex.RUnlock()

	return canCount
}

// spawnMultiple is our cantFunc which is run when canCount() returns less than
// desired number of jobs.
//
// If there is enough quota to spawn new servers, and we are not already in the
// middle of spawning too many servers, we spawn instances in the background.
func (s *opst) spawnMultiple(ctx context.Context, desired int, cmd string, req *Requirements, call string) {
	s.spawnMutex.Lock()
	defer s.spawnMutex.Unlock()
	var spawningTotal int
	var spawningCmd int
	for thisCmd, spawning := range s.spawningNow {
		spawningTotal += spawning
		if thisCmd == cmd {
			spawningCmd = spawning
		}
	}
	if s.config.SimultaneousSpawns > 0 && spawningTotal >= s.config.SimultaneousSpawns {
		clog.Debug(ctx, "spawnMultiple is spawning max servers already")
		return
	}

	requestedOS, requestedScript, requestedConfigFiles, requestedFlavor, needsSharedDisk, err := s.serverReqs(ctx, req)
	if err != nil {
		clog.Warn(ctx, "Failed to determine server requirements", "err", err)
		return
	}
	reqForSpawn := s.reqForSpawn(req)

	// work out how many we should spawn at once
	spawnable, flavor := s.checkQuota(ctx, reqForSpawn, requestedFlavor, call)
	if spawnable == 0 {
		clog.Debug(ctx, "spawnMultiple can't spawn due to lack of quota")
		return
	}
	perServer := flavor.HasSpaceFor(reqForSpawn.Cores, reqForSpawn.RAM, 0) // servers we spawn can have more disk than in the flavor, so we don't consider reqForSpawn.Disk here
	if perServer == 0 {
		clog.Error(ctx, "determined flavor doesn't have space for req", "flavor", flavor, "req", reqForSpawn)
		return
	}
	todo := int(math.Ceil(float64(desired) / float64(perServer)))
	needed := todo - spawningCmd
	if needed <= 0 {
		clog.Debug(ctx, "spawnMultiple is spawning enough for cmd already", "cmd", cmd, "todo", todo, "already", spawningCmd)
		return
	}
	todo = needed
	if spawnable < todo {
		todo = spawnable
	}
	var allowed int
	if s.config.SimultaneousSpawns > 0 {
		allowed = s.config.SimultaneousSpawns - spawningTotal
		if allowed < todo {
			todo = allowed
		}
	}

	// spawn servers in the background
	clog.Debug(ctx, "spawnMultiple will spawn new servers", "cmd", cmd, "desired", desired, "perserver",
		perServer, "spawnable", spawnable, "allowed", allowed, "already", spawningCmd, "actual", todo)
	for i := 0; i < todo; i++ {
		s.spawningNow[cmd]++
		go func() {
			defer internal.LogPanic(ctx, "spawnMultiple", false)

			s.spawn(ctx, reqForSpawn, flavor, requestedOS, requestedScript, requestedConfigFiles, needsSharedDisk, cmd, call)

			s.spawnMutex.Lock()
			s.spawningNow[cmd]--
			if s.spawningNow[cmd] <= 0 {
				delete(s.spawningNow, cmd)
			}
			s.spawnMutex.Unlock()

			errp := s.processQueue(ctx, "post spawn")
			if errp != nil {
				clog.Error(ctx, "processQueue recall failed", "err", errp)
			}
		}()
	}
}

// checkQuota sees if there's enough quota to spawn a server suitable for the
// given requirements.
//
// If requestedFlavor is nil, the smallest suitable server flavor will be
// determined.
//
// Returns the number of servers that can be spawned, and the flavor that should
// be spawned (if number greater than 0). Errors are simply Warn()ed.
func (s *opst) checkQuota(ctx context.Context, req *Requirements, requestedFlavor *cloud.Flavor, call string) (int, *cloud.Flavor) {
	s.resourceMutex.RLock()
	defer s.resourceMutex.RUnlock()

	flavor := requestedFlavor
	var err error
	if flavor == nil {
		flavor, err = s.determineFlavor(ctx, req, call)
		if err != nil {
			clog.Warn(ctx, "Failed to determine a server flavor", "err", err)
			return 0, nil
		}
	}

	quota, err := s.provider.GetQuota(ctx) // this includes resources used by currently spawning servers
	if err != nil {
		clog.Warn(ctx, "Failed to GetQuota", "err", err)
		return 0, nil
	}
	remainingInstances := unquotadVal
	if quota.MaxInstances > 0 {
		remainingInstances = quota.MaxInstances - quota.UsedInstances - s.reservedInstances
		if remainingInstances < 1 {
			clog.Debug(ctx, "lack of instance quota", "remaining", remainingInstances, "max", quota.MaxInstances,
				"used", quota.UsedInstances, "reserved", s.reservedInstances)
			s.notifyMessage("OpenStack: Not enough instance quota to create another server")
		}
	}
	if remainingInstances > 0 && s.quotaMaxInstances > -1 && s.quotaMaxInstances < quota.MaxInstances {
		// also check that the users configured max instances hasn't been breached
		s.serversMutex.RLock()
		numServers := len(s.servers)
		s.serversMutex.RUnlock()
		used := numServers + s.reservedInstances
		remaining := s.quotaMaxInstances - used
		if remaining < remainingInstances {
			remainingInstances = remaining
		}
		if remainingInstances < 1 {
			clog.Debug(ctx, "instances over configured max", "remaining", remainingInstances, "configuredMax",
				s.quotaMaxInstances, "usedPersonally", numServers, "reserved", s.reservedInstances)
		}
	}
	remainingRAM := unquotadVal
	if quota.MaxRAM > 0 {
		remainingRAM = quota.MaxRAM - quota.UsedRAM - s.reservedRAM
		if remainingRAM < flavor.RAM {
			clog.Debug(ctx, "lack of ram quota", "remaining", remainingRAM, "max", quota.MaxRAM, "used", quota.UsedRAM,
				"reserved", s.reservedRAM)
			s.notifyMessage(fmt.Sprintf("OpenStack: Not enough RAM quota to create another server (need %d, have %d)", flavor.RAM, remainingRAM))
		}
	}
	remainingCores := unquotadVal
	if quota.MaxCores > 0 {
		remainingCores = quota.MaxCores - quota.UsedCores - s.reservedCores
		if remainingCores < flavor.Cores {
			clog.Debug(ctx, "lack of cores quota", "remaining", remainingCores, "max", quota.MaxCores, "used", quota.UsedCores,
				"reserved", s.reservedCores)
			s.notifyMessage(fmt.Sprintf("OpenStack: Not enough cores quota to create another server (need %d, have %d)", flavor.Cores, remainingCores))
		}
	}
	remainingVolume := unquotadVal
	checkVolume := req.Disk > flavor.Disk // we'll only use up volume if we need more than the flavor offers
	if quota.MaxVolume > 0 && checkVolume {
		remainingVolume = quota.MaxVolume - quota.UsedVolume - s.reservedVolume
		if remainingVolume < req.Disk {
			clog.Debug(ctx, "lack of volume quota", "remaining", remainingVolume, "max", quota.MaxVolume, "used",
				quota.UsedVolume, "reserved", s.reservedVolume)
			s.notifyMessage(fmt.Sprintf("OpenStack: Not enough volume quota to create another server (need %d, have %d)", flavor.Disk, remainingVolume))
		}
	}
	if remainingInstances < 1 || remainingRAM < flavor.RAM || remainingCores < flavor.Cores || remainingVolume < req.Disk {
		return 0, nil
	}

	// (we only care that we can spawn at least 1, but calculate the actual
	// spawnable number in case we want to spawn multiple at once in the future)
	spawnable := remainingInstances
	if spawnable > 1 {
		n := remainingRAM / flavor.RAM // dividing ints == floor
		if n < spawnable {
			spawnable = n
		}
		n = remainingCores / flavor.Cores
		if n < spawnable {
			spawnable = n
		}
		if checkVolume {
			n = remainingVolume / req.Disk
			if n < spawnable {
				spawnable = n
			}
		}
	}
	return spawnable, flavor
}

// reqForSpawn checks the input Requirements and if the configured OSRAM (or
// overriding that, the Requirements.Other["cloud_os_ram"]) is higher that the
// Requirements.RAM, or Requirements.Disk is not set and OSDisk is configured,
// returns a new Requirements with the higher RAM/ configured Disk value.
// Otherwise returns the input.
func (s *opst) reqForSpawn(req *Requirements) *Requirements {
	reqForSpawn := req

	var osRAM int
	if val, defined := req.Other["cloud_os_ram"]; defined {
		i, err := strconv.Atoi(val)
		if err == nil {
			osRAM = i
		} else {
			osRAM = s.config.OSRAM
		}
	} else {
		osRAM = s.config.OSRAM
	}

	if req.RAM < osRAM {
		reqForSpawn = &Requirements{
			RAM:   osRAM,
			Time:  req.Time,
			Cores: req.Cores,
			Disk:  req.Disk,
			Other: req.Other,
		}
	}

	disk := req.Disk
	if disk == 0 {
		disk = s.config.OSDisk
	}
	if req.Disk < disk {
		reqForSpawn = &Requirements{
			RAM:   reqForSpawn.RAM,
			Time:  reqForSpawn.Time,
			Cores: reqForSpawn.Cores,
			Disk:  disk,
			Other: reqForSpawn.Other,
		}
	}

	return reqForSpawn
}

// spawn creates a new instance in OpenStack. Errors are not returned but are
// logged, and problematic servers are terminated.
func (s *opst) spawn(ctx context.Context, req *Requirements, flavor *cloud.Flavor, requestedOS string, requestedScript []byte,
	requestedConfigFiles string, needsSharedDisk bool, cmd string, call string) {
	volumeAffected := req.Disk > flavor.Disk

	// because spawning can take a while, we record that we're going to use
	// up some of our quota and unlock so other things can proceed
	s.resourceMutex.Lock()
	s.reservedInstances++
	s.reservedCores += flavor.Cores
	s.reservedRAM += flavor.RAM
	if volumeAffected {
		s.reservedVolume += req.Disk
	}
	s.resourceMutex.Unlock()

	// later on, immediately after the spawn request goes through (and so
	// presumably is using up quota), but before the new server powers up,
	// drop our reserved values down or we'll end up double-counting
	// resource usage in checkQuota(), since that takes in to account
	// resources used by an in-progress spawn.
	usingQuotaCB := func() {
		s.resourceMutex.Lock()
		s.reservedInstances--
		s.reservedCores -= flavor.Cores
		s.reservedRAM -= flavor.RAM
		if volumeAffected {
			s.reservedVolume -= req.Disk
		}
		s.resourceMutex.Unlock()
	}

	var osUser string
	if val, defined := req.Other["cloud_user"]; defined {
		osUser = val
	} else {
		osUser = s.config.OSUser
	}

	// *** we need a better way for our test script to prove the bugs that rely
	// on debugEffect, that doesn't affect non-testing code. Probably have to
	// mock OpenStack instead at some point...
	var thisDebugCount int
	if debugEffect != "" {
		debugCounter++
		thisDebugCount = debugCounter
	}
	if debugEffect == "slowSecondSpawn" && thisDebugCount == 3 {
		<-time.After(10 * time.Second)
	}

	// spawn
	failMsg := "server failed spawn"

	clog.Debug(ctx, "will spawn new server", "cmd", cmd)

	tSpawn := time.Now()
	server, err := s.provider.Spawn(ctx, requestedOS, osUser, flavor.ID, req.Disk, s.config.ServerKeepTime,
		false, usingQuotaCB)

	clog.Debug(ctx, "spawned server", "took", time.Since(tSpawn))

	if err == nil && server != nil {
		// wait until boot is finished, ssh is ready and osScript has
		// completed
		clog.Debug(ctx, "waiting for server to become ready")
		failMsg = "server failed ready"
		tReady := time.Now()
		err = s.actOnServerIfNeeded(server, cmd, func(ctx context.Context) error {
			return server.WaitUntilReady(ctx, requestedConfigFiles, requestedScript)
		})
		clog.Debug(ctx, "waited for server to become ready", "took", time.Since(tReady), "err", err)

		if err == nil && needsSharedDisk {
			s.serversMutex.RLock()
			localhostIP := s.servers[localhostName].IP
			s.serversMutex.RUnlock()
			err = s.actOnServerIfNeeded(server, cmd, func(ctx context.Context) error { return server.MountSharedDisk(context.Background(), localhostIP) })
		}

		if err == nil {
			failMsg = "server failed uploads"

			// check that the exe of the cmd we're supposed to run exists on the
			// new server, and if not, copy it over *** this is just a hack to
			// get wr working, need to think of a better way of doing this...
			exe := strings.Split(cmd, " ")[0]
			var exePath string
			if exePath, err = exec.LookPath(exe); err == nil {
				stdCh := make(chan string)
				err = s.actOnServerIfNeeded(server, cmd, func(ctx context.Context) error {
					std, _, errRun := server.RunCmd(ctx, "file "+exePath, false)
					go func() {
						stdCh <- std
					}()
					return errRun
				})
				stdout := <-stdCh
				if stdout != "" {
					if strings.Contains(stdout, "No such file") {
						// *** NB this will fail if exePath is in a dir we can't
						// create on the remote server, eg. if it is in our home
						// dir, but the remote server has a different user, or
						// presumably if it is somewhere requiring root
						// permission
						err = s.actOnServerIfNeeded(server, cmd, func(ctx context.Context) error { return server.UploadFile(ctx, exePath, exePath) })
						if err == nil {
							err = s.actOnServerIfNeeded(server, cmd, func(ctx context.Context) error {
								_, _, errRun := server.RunCmd(ctx, "chmod u+x "+exePath, false)
								return errRun
							})
						} else if err.Error() != serverNotNeededErrStr {
							err = fmt.Errorf("could not upload exe [%s]: %s (try putting the exe in /tmp?)", exePath, err)
						}
					} else if err != nil && err.Error() != serverNotNeededErrStr {
						err = fmt.Errorf("could not check exe with [file %s]: %s [%s]", exePath, stdout, err)
					}
				} else {
					// checking for exePath with the file command failed for
					// some reason, and without any stdout... but let's just
					// try the upload anyway, assuming the exe isn't there
					err = s.actOnServerIfNeeded(server, cmd, func(ctx context.Context) error { return server.UploadFile(ctx, exePath, exePath) })
					if err == nil {
						err = s.actOnServerIfNeeded(server, cmd, func(ctx context.Context) error {
							_, _, errRun := server.RunCmd(ctx, "chmod u+x "+exePath, false)
							return errRun
						})
					} else if err.Error() != serverNotNeededErrStr {
						err = fmt.Errorf("could not upload exe [%s]: %s (try putting the exe in /tmp?)", exePath, err)
					}
				}
			} else {
				err = fmt.Errorf("could not look for exe [%s]: %s", exePath, err)
			}
		}
	}

	if debugEffect == "failFirstSpawn" && thisDebugCount == 1 {
		err = errors.New("forced fail")
	}

	if s.cleanedUp() {
		err = errors.New(serverNotNeededErrStr)
	}

	// handle Spawn() or upload-of-exe errors now, by destroying the server
	// and noting we failed
	if err != nil {
		if err.Error() == serverNotNeededErrStr {
			clog.Debug(ctx, failMsg, "err", err)
		} else {
			clog.Warn(ctx, failMsg, "err", err)
		}
		if server != nil {
			errd := server.Destroy(ctx)
			if errd != nil {
				clog.Debug(ctx, "server also failed to destroy", "err", errd)
			}
		} else if s.provider.ErrIsNoHardware(err) {
			s.ffCache.Set(flavor.ID, true, cache.DefaultExpiration)
			clog.Warn(ctx, "server failed to spawn due to lack of hardware")
		}
		if err.Error() != serverNotNeededErrStr {
			s.notifyMessage(fmt.Sprintf("OpenStack: Failed to create a usable server: %s", err))
		}
		return
	}

	if _, failed := s.ffCache.Get(flavor.ID); failed {
		clog.Debug(ctx, "server successfully spawned on previously failed flavor")
		s.ffCache.Delete(flavor.ID)
	}

	s.serversMutex.Lock()
	s.spawnedServers[server.ID] = server
	s.serversMutex.Unlock()
	clog.Debug(ctx, "server became usable")
}

// actOnServerIfNeeded runs the given code unless cleanup() has been called, or
// cmd no longer needs to be run, in which case an error is returned instead.
// It will also periodiclly check if the cmd still needs to be run, and return
// early with an error if not, even while the given code is still running.
func (s *opst) actOnServerIfNeeded(server *cloud.Server, cmd string, code func(ctx context.Context) error) error {
	if s.cleanedUp() {
		return errors.New(serverNotNeededErrStr)
	}

	s.scMutex.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		s.scMutex.Lock()
		delete(s.spawnCanceller[cmd], server.ID)
		s.scMutex.Unlock()
	}()
	canceller := make(chan struct{}, 1)
	if _, exists := s.spawnCanceller[cmd]; !exists {
		s.spawnCanceller[cmd] = make(map[string]chan struct{})
	}
	s.spawnCanceller[cmd][server.ID] = canceller
	s.scMutex.Unlock()

	if s.cmdCountRemaining(cmd) <= 0 {
		clog.Debug(ctx, "bailing on a spawn early since no longer needed", "server", server.ID)
		return errors.New(serverNotNeededErrStr)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- code(ctx)
	}()

	for {
		select {
		case err := <-errCh:
			return err
		case <-canceller:
			cancel()
			clog.Debug(ctx, "bailing on a spawn mid-action since no longer needed", "server", server.ID)
			return errors.New(serverNotNeededErrStr)
		}
	}
}

// cmdNotNeeded cancels the context set by actOnServerIfNeeded(), if any.
func (s *opst) cmdNotNeeded(cmd string) {
	s.scMutex.Lock()
	defer s.scMutex.Unlock()
	if serverMap, exists := s.spawnCanceller[cmd]; exists {
		delete(s.spawnCanceller, cmd)
		for _, canceller := range serverMap {
			close(canceller)
		}
	}
}

// runCmd runs the command on next available server. NB: we only return an error
// if we can't start the cmd, not if the command fails (schedule() only
// guarantees that the cmds are run count times, not that they are /successful/
// that many times).
func (s *opst) runCmd(ctx context.Context, cmd string, req *Requirements, reservedCh chan bool, call string) error {
	requestedOS, requestedScript, requestedConfigFiles, requestedFlavor, needsSharedDisk, err := s.serverReqs(ctx, req)
	if err != nil {
		return err
	}

	if s.cleanedUp() {
		reservedCh <- false
		return nil
	}

	// look through space on existing servers to see if we can run cmd on one
	// of them
	s.serversMutex.RLock()
	var server *cloud.Server
	for sid, thisServer := range s.servers {
		if !thisServer.IsBad() && thisServer.Matches(requestedOS, requestedScript, requestedConfigFiles, requestedFlavor,
			needsSharedDisk) && thisServer.Allocate(ctx, req.Cores, req.RAM, req.Disk) {
			server = thisServer

			// *** reservedCh is buffered and sending on it should never
			// block, but somehow we have gotten stuck here before; make
			// sure we don't get stuck on this send
			ch := make(chan bool, 1)
			done := make(chan bool, 1)
			go func() {
				reservedCh <- true
				done <- true
				ch <- true
			}()
			go func() {
				select {
				case <-time.After(reserveChTimeout):
					ch <- false
				case <-done:
					return
				}
			}()
			sentReserved := <-ch
			if !sentReserved {
				clog.Warn(ctx, "failed to send on reservedCh", "server", sid)
			}

			clog.Debug(ctx, "picked server")
			break
		}
	}
	s.serversMutex.RUnlock()

	if server == nil {
		reservedCh <- false
		return errors.New("no available server")
	}

	// later, after we've run the command, this server will be available for
	// another; release resources, and local scheduler will trigger a new
	// processQueue() call
	defer func() {
		if !server.Destroyed() && server.PermanentProblem() == "" {
			server.Release(ctx, req.Cores, req.RAM, req.Disk)
		}
	}()

	// now we have a server, ssh over and run the cmd on it
	if server.Name == localhostName {
		clog.Debug(ctx, "running command locally", "cmd", cmd)
		reserved := make(chan bool)
		go func() {
			<-reserved
		}()

		err = s.local.runCmd(ctx, cmd, req, reserved, call)
	} else {
		if s.config.Umask > 0 {
			cmd = fmt.Sprintf("(umask %d && %s)", s.config.Umask, cmd)
		}
		clog.Debug(ctx, "running command remotely", "cmd", cmd)
		_, _, err = server.RunCmd(context.Background(), cmd, false)

		// if we got an error running the command, we won't use this server
		// again
		if err != nil {
			if !server.Destroyed() {
				// tell the user about why we're not using this server, but
				// don't just Destroy it: let them investigate the server
				// manually if they wish, and let them Destroy when they wish.
				server.GoneBad(err.Error())
				s.notifyBadServer(server)
				clog.Warn(ctx, "server went bad, won't be used again")
			}
		}
	}
	if err == nil {
		clog.Debug(ctx, "ran command", "cmd", cmd)
	} else {
		clog.Warn(ctx, "failed to run command", "cmd", cmd, "err", err)
	}
	return err
}

// stateUpdate checks all our servers are really alive, and adds newly spawned
// servers to the map that runCmd will check.
func (s *opst) stateUpdate(ctx context.Context) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// when spawn() has finished creating a server and it is usable, it doesn't
	// immediately add it to s.servers map, since if this happens during a
	// processQueue() call then we could break bin-packing, with low priority
	// jobs getting allocated to the new server. Instead spawn() adds the new
	// server to spawnedServers, and now we move them to servers, since this
	// method is called once at the start of processQueue()
	s.serversMutex.Lock()
	for id, server := range s.spawnedServers {
		s.servers[id] = server
		delete(s.spawnedServers, id)
		clog.Debug(ctx, "made server eligible for use", "id", id)
	}
	s.serversMutex.Unlock()

	var servers []*cloud.Server
	s.serversMutex.Lock()
	for _, server := range s.servers {
		if server.ID != "" {
			if server.Destroyed() {
				delete(s.servers, server.ID)
				continue
			}
			servers = append(servers, server)
		}
	}
	s.serversMutex.Unlock()

	if s.updatingState {
		return
	}
	s.updatingState = true

	// stateUpdate must return quickly, but checking on the servers with the
	// Alive() call can take too long, so we do the rest in a goroutine
	go func() {
		defer internal.LogPanic(ctx, "stateUpdate", true)

		for _, server := range servers {
			if server.Destroyed() {
				continue
			}

			alive := server.Alive(ctx, true)
			if server.IsBad() {
				// check if the server is fine now
				if alive && server.PermanentProblem() == "" {
					worked := server.NotBad()
					if worked {
						s.notifyBadServer(server)
						clog.Debug(ctx, "server became good", "server", server.ID)
					}
				}
			} else if !alive {
				server.GoneBad()
				s.notifyBadServer(server)
				clog.Debug(ctx, "server went bad", "server", server.ID)
			}
		}

		s.stateMutex.Lock()
		defer s.stateMutex.Unlock()
		s.updatingState = false
	}()
}

// postProcess checks that all our newly spawned servers have been used, and if
// not, initiates the countdown to their destruction
func (s *opst) postProcess(ctx context.Context) {
	s.serversMutex.Lock()
	for _, server := range s.servers {
		if server.Name != localhostName && !server.Used() {
			clog.Debug(ctx, "placing unused server on deathrow", "server", server.ID)
			server.Allocate(ctx, 0, 1, 1)
			server.Release(ctx, 0, 1, 1)
		}
	}
	s.serversMutex.Unlock()
}

// recover achieves the aims of Recover(). Here we find the given host, and
// start tracking it to know when it is no longer running any of the given cmds
// for it, at which point we destroy it. If the supplied UserName for the host
// is wrong, or we otherwise can't ssh to it, the host will be destroyed
// immediately. NB: the host checking only works on machines with the 'pgrep'
// command, such as linux etc.
func (s *opst) recover(ctx context.Context, cmd string, req *Requirements, host *RecoveredHostDetails) error {
	server := s.provider.GetServerByName(host.Host)
	if server == nil {
		clog.Warn(ctx, "recover called for non-existent server", "host", host)
		return nil
	}

	if host.TTD == 0 {
		// we keep servers for ever, so no need to monitor it
		return nil
	}

	server.UserName = host.UserName
	if !server.Alive(ctx, true) {
		clog.Warn(ctx, "recover called for server that is not alive (or username was wrong?)", "host", host.Host)
		errd := server.Destroy(ctx)
		if errd != nil {
			clog.Warn(ctx, "recovered server destruction failed", "server", server.ID, "err", errd)
		}
		return nil
	}

	s.rsMutex.Lock()
	defer s.rsMutex.Unlock()

	if s.recoveredServers[host.Host] {
		return nil
	}

	// *** we will only check against the first 2 words of cmd, which for our
	// purposes of wr will be 'wr runner'. This lets us do a single check per
	// server, and reduces possible issues with trying to get a process name
	// match on a long, complex command line. However it might not work properly
	// with the arbitrary commands that people could in theory schedule
	cmdSplit := strings.Split(cmd, " ")
	cmd = cmdSplit[0]
	if len(cmdSplit) > 1 {
		cmd += " " + cmdSplit[1]
	}

	go func() {
		defer internal.LogPanic(ctx, "recover", true)
		clog.Debug(ctx, "recovered server will be checked for running jobs periodically", "server", server.ID)

		// periodically check on this server; when it is no longer running
		// anything, destroy it
		ticker := time.NewTicker(host.TTD)
		for {
			select {
			case <-ticker.C:
				active := true
				so, se, errr := server.RunCmd(context.Background(), "pgrep -f '"+cmd+"'", false)
				if errr != nil {
					// *** assume the error is because a process with cmd
					// doesn't exist, not because prgrep failed for some other
					// reason
					clog.Debug(ctx, "recovered server is no longer running anything", "server", server.ID, "checkCmd",
						"pgrep -f '"+cmd+"'", "stdout", so, "stderr", se, "err", errr)
					active = false
				}

				if !active {
					ticker.Stop()

					errd := server.Destroy(ctx)
					if errd != nil {
						clog.Warn(ctx, "recovered server destruction failed", "server", server.ID, "err", errd)
					} else {
						clog.Debug(ctx, "recovered server was destroyed after going idle", "server", server.ID)
					}

					errp := s.processQueue(ctx, "openstack recover")
					if errp != nil {
						clog.Error(ctx, "processQueue call after recovery failed", "err", errp)
					}

					return
				}
			case <-s.stopRSMonitoring:
				ticker.Stop()
				return
			}
		}
	}()

	s.recoveredServers[host.Host] = true
	return nil
}

// hostToID does the necessary lookup to convert hostname to instance id.
func (s *opst) hostToID(host string) string {
	server := s.provider.GetServerByName(host)
	if server == nil {
		return ""
	}
	return server.ID
}

// getHost returns a cloud.Server for the given host.
func (s *opst) getHost(host string) (Host, bool) {
	server := s.provider.GetServerByName(host)
	if server == nil {
		return nil, false
	}

	return server, true
}

// setMessageCallBack sets the given callback.
func (s *opst) setMessageCallBack(ctx context.Context, cb MessageCallBack) {
	s.cbmutex.Lock()
	defer s.cbmutex.Unlock()
	s.msgCB = cb
}

// notifyMessage calls the message callback with the given message in a
// goroutine, if that callback has been set.
func (s *opst) notifyMessage(msg string) {
	s.cbmutex.RLock()
	defer s.cbmutex.RUnlock()
	if s.msgCB != nil {
		go s.msgCB(msg)
	}
}

// setBadServerCallBack sets the given callback.
func (s *opst) setBadServerCallBack(ctx context.Context, cb BadServerCallBack) {
	s.cbmutex.Lock()
	defer s.cbmutex.Unlock()
	s.badServerCB = cb
}

// notifyBadServer calls the bad server callback with the given server in a
// goroutine, if that callback has been set.
func (s *opst) notifyBadServer(server *cloud.Server) {
	s.cbmutex.RLock()
	defer s.cbmutex.RUnlock()
	if s.badServerCB != nil {
		go s.badServerCB(server)
	}
}

// cleanup destroys our internal queues and brings down our servers.
func (s *opst) cleanup(ctx context.Context) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	s.cleanMutex.Lock()
	defer s.cleanMutex.Unlock()
	s.spawnMutex.Lock()
	defer s.spawnMutex.Unlock()

	// prevent any further scheduling and queue processing, and destroy the
	// queue
	s.cleaned = true
	err := s.queue.Destroy()
	if err != nil {
		clog.Warn(ctx, "cleanup queue destruction failed", "err", err)
	}

	// wait for any ongoing state update to complete
	s.stateMutex.Lock()
	for {
		if !s.updatingState {
			break
		}
		s.stateMutex.Unlock()
		<-time.After(10 * time.Millisecond)
		s.stateMutex.Lock()
	}
	defer s.stateMutex.Unlock()

	// bring down all our servers
	s.serversMutex.Lock()
	close(s.stopRSMonitoring)
	for id, server := range s.spawnedServers {
		s.servers[id] = server
		delete(s.spawnedServers, id)
	}
	for sid, server := range s.servers {
		if sid == localhostName {
			continue
		}

		errd := server.Destroy(ctx)
		if errd != nil {
			clog.Warn(ctx, "cleanup server destruction failed", "server", server.ID, "err", errd)
		}
		delete(s.servers, sid)
	}
	defer s.serversMutex.Unlock()

	// teardown any cloud resources created
	err = s.provider.TearDown(ctx)
	if err != nil && !strings.Contains(err.Error(), "nothing to tear down") {
		clog.Warn(ctx, "cleanup teardown failed", "err", err)
	}
}
