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

/*
Package scheduler lets the jobqueue server interact with the configured job
scheduler (if any) to submit jobqueue runner clients and have them run on a
compute cluster (or local machine).

Currently implemented schedulers are local, LSF, OpenStack and Kubernetes. The
implementation of each supported scheduler type is in its own .go file.

It's a pseudo plug-in system in that it is designed so that you can easily add a
go file that implements the methods of the scheduleri interface, to support a
new job scheduler. On the other hand, there is no dynamic loading of these go
files; they are all imported (they all belong to the scheduler package), and the
correct one used at run time. To "register" a new scheduleri implementation you
must add a case for it to New() and rebuild.

    import "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
    s, err := scheduler.New("local", &scheduler.ConfigLocal{"bash"})
    req := &scheduler.Requirements{RAM: 300, Time: 2 * time.Hour, Cores: 1}
    err = s.Schedule("myWRRunnerClient -args", req, 24)
    // wait, and when s.Busy() returns false, your command has been run 24 times
*/
package scheduler

import (
	"context"
	"crypto/md5" // #nosec - not used for cryptographic purposes here
	"fmt"
	"sort"
	"strconv"
	"time"

	sync "github.com/sasha-s/go-deadlock"
	"github.com/wtsi-ssg/wr/clog"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/dgryski/go-farm"
)

const (
	defaultReserveTimeout               = 1 // implementers of reserveTimeout can just return this
	infiniteQueueTime     time.Duration = 0
	minimumQueueTime      time.Duration = 1 * time.Minute
)

// Err* constants are found in the returned Errors under err.Err, so you can
// cast and check if it's a certain type of error.
var (
	ErrBadScheduler = "unknown scheduler name"
	ErrImpossible   = "scheduler cannot accept the job, since its resource requirements are too high"
	ErrBadFlavor    = "unknown server flavor"
)

// Error records an error and the operation and scheduler that caused it.
type Error struct {
	Scheduler string // the scheduler's Name
	Op        string // name of the method
	Err       string // one of our Err* vars
}

func (e Error) Error() string {
	return "scheduler(" + e.Scheduler + ") " + e.Op + "(): " + e.Err
}

// Requirements describes the resource requirements of the commands you want to
// run, so that when provided to a scheduler it will be able to schedule things
// appropriately.
type Requirements struct {
	RAM      int               // the expected peak RAM in MB Cmd will use while running
	Time     time.Duration     // the expected time Cmd will take to run
	Cores    float64           // how many processor cores the Cmd will use
	Disk     int               // the required local disk space in GB the Cmd needs to run
	Other    map[string]string // a map that will be passed through to the job scheduler, defining further arbitrary resource requirements
	CoresSet bool              // to distinguish between you specifying 0 Cores and not specifying Cores at all
	DiskSet  bool              // to distinguish between you specifying 0 Disk and not specifying Disk at all
	OtherSet bool
}

// Stringify represents the contents of the Requirements as a string, sorting
// the keys of Other to ensure the same result is returned for the same content
// every time. Note that the data in Other undergoes a 1-way transformation,
// so you cannot recreate the Requirements from the the output of this method.
func (req *Requirements) Stringify() string {
	var other string
	if len(req.Other) > 0 {
		otherKeys := make([]string, 0, len(req.Other))
		for key := range req.Other {
			otherKeys = append(otherKeys, key)
		}
		sort.Strings(otherKeys)
		for _, key := range otherKeys {
			other += ":" + key + "=" + req.Other[key]
		}

		// now convert it all in to an md5sum, to avoid any problems with some
		// key values having line returns etc. *** we might like to use
		// byteKey() from jobqueue package instead, but that isn't exported...
		other = fmt.Sprintf(":%x", md5.Sum([]byte(other))) // #nosec
	}

	return fmt.Sprintf("%d:%.0f:%s:%d%s", req.RAM, req.Time.Minutes(), strconv.FormatFloat(req.Cores, 'f', -1, 64), req.Disk, other)
}

// Clone creates a copy of the Requirements.
func (req *Requirements) Clone() *Requirements {
	new := &Requirements{
		RAM:      req.RAM,
		Time:     req.Time,
		Cores:    req.Cores,
		CoresSet: req.CoresSet,
		Disk:     req.Disk,
		DiskSet:  req.DiskSet,
		OtherSet: req.OtherSet,
	}
	if req.OtherSet || len(req.Other) > 0 {
		newOther := make(map[string]string, len(req.Other))
		for key, val := range req.Other {
			newOther[key] = val
		}
		new.Other = newOther
	}
	return new
}

// CmdStatus lets you describe how many of a given cmd are already in the job
// scheduler, and gives the details of those jobs.
type CmdStatus struct {
	Count   int
	Running [][2]int // a slice of [id, index] tuples
	Pending [][2]int // ditto
	Other   [][2]int // ditto, for jobs in some strange state
}

// MessageCallBack functions receive a message that would be good to display to
// end users, so they understand current error conditions related to the
// scheduler.
type MessageCallBack func(msg string)

// BadServerCallBack functions receive a server when a cloud scheduler discovers
// that a server it spawned no longer seems functional. It's possible that this
// was due to a temporary networking issue, in which case the callback will be
// called again with the same server when it is working fine again: check
// server.IsBad(). If it's bad, you'd probably call server.Destroy() after
// confirming the server is definitely unusable (eg. ask the end user to
// manually check).
type BadServerCallBack func(server *cloud.Server)

// RecoveredHostDetails lets you describe a host for supplying to Recover(). Not
// all fields are relevant for all schedulers. Some might use none, so a nil
// RecoveredHostDetails might be valid. Cloud schedulers need all fields
// specified.
type RecoveredHostDetails struct {
	Host     string        // host's hostname
	UserName string        // username needed to ssh log in to host
	TTD      time.Duration // frequency to check if the host is idle, and if so destroy it
}

// Host interface let's us run a command on a local or remote host.
type Host interface {
	// RunCmd runs the given cmd on the host, optionally in the background,
	// cancellable with the context, returning stdout, stderr from the command,
	// or an error if running the command wasn't possible.
	RunCmd(ctx context.Context, cmd string, background bool) (stdout, stderr string, err error)
}

// scheduleri interface must be satisfied to add support for a particular job
// scheduler.
type scheduleri interface {
	initialize(ctx context.Context, config interface{}) error                                     // do any initial set up to be able to use the job scheduler
	schedule(ctx context.Context, cmd string, req *Requirements, priority uint8, count int) error // achieve the aims of Schedule()
	scheduled(ctx context.Context, cmd string) (int, error)                                       // achieve the aims of Scheduled()
	recover(ctx context.Context, cmd string, req *Requirements, host *RecoveredHostDetails) error // achieve the aims of Recover()
	busy(ctx context.Context) bool                                                                // achieve the aims of Busy()
	reserveTimeout(ctx context.Context, req *Requirements) int                                    // achieve the aims of ReserveTimeout()
	maxQueueTime(req *Requirements) time.Duration                                                 // achieve the aims of MaxQueueTime(), return 0 for infinite queue time
	hostToID(host string) string                                                                  // achieve the aims of HostToID()
	getHost(host string) (Host, bool)                                                             // get a Host that can be used to run commands over ssh on the given host, return false boolean if not such host exists
	setMessageCallBack(ctx context.Context, cb MessageCallBack)                                   // achieve the aims of SetMessageCallBack()
	setBadServerCallBack(ctx context.Context, cb BadServerCallBack)                               // achieve the aims of SetBadServerCallBack()
	cleanup(ctx context.Context)                                                                  // do any clean up once you've finished using the job scheduler
}

// CloudConfig interface could be satisfied by the config option taken by cloud
// schedulers which have a ConfigFiles property, a property for configuring a
// default ssh login username, and a property for determining how long to keep
// idle servers.
type CloudConfig interface {
	// AddConfigFile takes a value like that of the ConfigFiles property of the
	// struct implementing this interface, and appends this value to what is
	// in ConfigFiles, or sets it if unset.
	AddConfigFile(spec string)

	// GetOSUser returns the default ssh login username for servers.
	GetOSUser() string

	// GetServerKeepTime returns the time to keep idle servers alive for.
	GetServerKeepTime() time.Duration
}

// Scheduler gives you access to all of the methods you'll need to interact with
// a job scheduler.
type Scheduler struct {
	impl    scheduleri
	Name    string
	limiter map[string]int
	sync.Mutex
}

// New creates a new Scheduler to interact with the given job scheduler.
// Possible names so far are "lsf", "local", "openstack" and "kubernetes". You
// must also provide a config struct appropriate for your chosen scheduler, eg.
// for the local scheduler you will provide a ConfigLocal.
//
// Providing a logger allows for debug messages to be logged somewhere, along
// with any "harmless" or unreturnable errors. If not supplied, we use a default
// logger that discards all log messages.
func New(ctx context.Context, name string, config interface{}) (*Scheduler, error) {
	var s *Scheduler
	switch name {
	case "lsf":
		s = &Scheduler{impl: new(lsf)}
	case "local":
		s = &Scheduler{impl: new(local)}
	case "openstack":
		s = &Scheduler{impl: new(opst)}
	case "kubernetes":
		s = &Scheduler{impl: new(k8s)}
	default:
		return nil, Error{name, "New", ErrBadScheduler}
	}

	s.Name = name
	s.limiter = make(map[string]int)
	err := s.impl.initialize(s.typeContext(ctx), config)

	return s, err
}

// typeContext returns a context based on the scheduler type.
func (s *Scheduler) typeContext(ctx context.Context) context.Context {
	return clog.ContextWithSchedulerType(ctx, s.Name)
}

// SetMessageCallBack sets the function that will be called when a scheduler has
// some message that could be informative to end users wondering why something
// is not getting scheduled. The message typically describes an error condition.
func (s *Scheduler) SetMessageCallBack(ctx context.Context, cb MessageCallBack) {
	s.impl.setMessageCallBack(s.typeContext(ctx), cb)
}

// SetBadServerCallBack sets the function that will be called when a cloud
// scheduler discovers that one of the servers it spawned seems to no longer be
// functional or reachable. Only relevant for cloud schedulers.
func (s *Scheduler) SetBadServerCallBack(ctx context.Context, cb BadServerCallBack) {
	s.impl.setBadServerCallBack(s.typeContext(ctx), cb)
}

// Schedule gets your cmd scheduled in the job scheduler. You give it a command
// that you would like `count` identical instances of running via your job
// scheduler. If you already had `count` many scheduled, it will do nothing. If
// you had less than `count`, it will schedule more to run. If you have more
// than `count`, it will remove the appropriate number of scheduled (but not yet
// running) jobs that were previously scheduled for this same cmd (counts of 0
// are legitimate - it will get rid of all non-running jobs for the cmd).
//
// Typically schedulers will end up running cmds according to their "size" (cpu
// and memory needed as per the req), with larger cmds running first due to bin
// packing. Some schedulers will take the given priority in to account and try
// to run cmds with higher priorities before those with lower ones. Equal
// priority jobs will use the normal approach.
//
// If no error is returned, you know all `count` of your jobs are now scheduled
// and will eventually run unless you call Schedule() again with the same
// command and a lower count. NB: there is no guarantee that the jobs run
// successfully, and no feedback on their success or failure is given.
func (s *Scheduler) Schedule(ctx context.Context, cmd string, req *Requirements, priority uint8, count int) error {
	// Schedule may get called many times in different go routines, eg. a
	// succession of calls with the same cmd and req but decrementing count.
	// Here we arrange that impl.schedule is only called once at a time per
	// cmd: if not already running we call as normal; if running we don't run
	// it but return immediately while storing the more recent desired count;
	// when it finishes running, we re-run with the most recent count, if any
	s.Lock()
	if _, limited := s.limiter[cmd]; limited {
		s.limiter[cmd] = count
		s.Unlock()
		return nil
	}
	s.limiter[cmd] = count
	s.Unlock()

	err := s.impl.schedule(s.typeContext(ctx), cmd, req.Clone(), priority, count)

	s.Lock()
	if newcount, limited := s.limiter[cmd]; limited {
		if newcount != count {
			go func() {
				defer internal.LogPanic(ctx, "schedule recall", true)
				errf := s.Schedule(ctx, cmd, req, priority, newcount)
				if errf != nil {
					clog.Error(ctx, "schedule recall", "err", errf)
				}
			}()
		}
		delete(s.limiter, cmd)
	}
	s.Unlock()

	return err
}

// Scheduled tells you how many of the given cmd are currently scheduled in the
// scheduler.
func (s *Scheduler) Scheduled(ctx context.Context, cmd string) (int, error) {
	return s.impl.scheduled(s.typeContext(ctx), cmd)
}

// Recover is used if you had Scheduled some cmds, then you crashed, and now
// you're starting up again and want the scheduler to take in to account the
// fact that you still have some commands running on certain hosts. Doing this
// may allow us to avoid overcommitting resources or terminate unneeded hosts,
// if relevant for the scheduler in question. (For some schedulers, this does
// nothing.)
//
// The cmd and req ought to exactly match those previously supplied to
// Schedule() before your crash.
func (s *Scheduler) Recover(ctx context.Context, cmd string, req *Requirements, host *RecoveredHostDetails) error {
	return s.impl.recover(s.typeContext(ctx), cmd, req, host)
}

// Busy reports true if there are any Schedule()d cmds still in the job
// scheduler's system. This is useful when testing and other situations where
// you want to avoid shutting down the server while there are still clients
// running/ about to run.
func (s *Scheduler) Busy(ctx context.Context) bool {
	return s.impl.busy(s.typeContext(ctx))
}

// ReserveTimeout returns the number of seconds that runners spawned in this
// scheduler should wait for new jobs to appear in the manager's queue.
func (s *Scheduler) ReserveTimeout(ctx context.Context, req *Requirements) int {
	return s.impl.reserveTimeout(s.typeContext(ctx), req)
}

// MaxQueueTime returns the maximum amount of time that jobs with the given
// resource requirements are allowed to run for in the job scheduler's queue. If
// the job scheduler doesn't have a queue system, or if the queue allows jobs to
// run forever, then this returns req.Time + 15 mins.
func (s *Scheduler) MaxQueueTime(req *Requirements) time.Duration {
	d := s.impl.maxQueueTime(req)
	if d == 0 {
		// jobqueue Server uses this to pass a time limit to the client process
		// being scheduled, which we want to exit soon after it has done a
		// minimal amount of work, but not earlier than 1min to aid efficiency
		return req.Time + minimumQueueTime
	}
	return d
}

// HostToID will return the server id of the server with the given host name, if
// the scheduler is cloud based. Otherwise this just returns an empty string.
func (s *Scheduler) HostToID(host string) string {
	return s.impl.hostToID(host)
}

// ProcessNotRunngingOnHost will ssh to the given host and check if the given
// process id is still running. Returns true if it isn't. Returns false if it is
// running, or if the ssh wasn't possible. This is to find out if a process is
// really dead, or if there might just be a temporary networking problem where
// ssh might fail. The ssh attempt can be cancelled using the supplied context.
func (s *Scheduler) ProcessNotRunngingOnHost(ctx context.Context, pid int, hostName string) bool {
	host, ok := s.impl.getHost(hostName)
	if !ok {
		return false
	}

	stdo, _, err := host.RunCmd(ctx, fmt.Sprintf("ps -p %d | wc -l", pid), false)
	if err != nil || stdo != "1\n" {
		return false
	}

	return true
}

// Cleanup means you've finished using a scheduler and it can delete any
// remaining jobs in its system and clean up any other used resources.
func (s *Scheduler) Cleanup(ctx context.Context) {
	s.impl.cleanup(s.typeContext(ctx))
}

// jobName could be useful to a scheduleri implementer if it needs a constant-
// width (length 36) string unique to the cmd and deployment, and optionally
// suffixed with a random string (length 9, total length 45).
func jobName(cmd string, deployment string, unique bool) string {
	l, h := farm.Hash128([]byte(cmd))
	name := fmt.Sprintf("wr%s_%016x%016x", deployment[0:1], l, h)

	if unique {
		name += "_" + internal.RandomString()
	}

	return name
}
