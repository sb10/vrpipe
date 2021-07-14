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

// This file contains a scheduleri implementation for 'local': running jobs
// on the local machine directly. It has a very simple strictly fifo queue, so
// may not be very efficient with the machine's resources.

import (
	"context"
	"io"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	sync "github.com/sasha-s/go-deadlock"
	"github.com/wtsi-ssg/wr/clog"

	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/queue"
	"github.com/inconshreveable/log15"
	logext "github.com/inconshreveable/log15/ext"
	"github.com/shirou/gopsutil/process"
)

const (
	localPlace          = "localhost"
	localReserveTimeout = 1
	priorityScaler      = float64(255) / float64(100)
	reserveChTimeout    = 30 * time.Second
)

// cmdProcessSanitiser is used to make cmds look like their process
// representation
var cmdProcessSanitiser = strings.NewReplacer("'", "")

// reqCheckers are functions used by schedule() to see if it is at all possible
// to ever run a job with the given resource requirements. (We make use of this
// in the local struct so that other implementers of scheduleri can embed local,
// use local's schedule(), but have their own reqChecker implementation.)
type reqChecker func(ctx context.Context, req *Requirements) error

// maxResourceGetter are functions used by schedule() to see what the maximum of
// of a resource like memory or time is. (We make use of this in the local
// struct so that other implementers of scheduleri can embed local, use local's
// schedule(), but have their own maxResourceGetter implementation.)
type maxResourceGetter func() int

// canCounters are functions used by processQueue() to see how many of a job
// can be run. (We make use of this in the local struct so that other
// implementers of scheduleri can embed local, use local's processQueue(), but
// have their own canCounter implementation.) The call argument will be a random
// string. That same string will also be supplied to the cmdRunner function, so
// you can tie together cmdRunner invocations that are all a result of a
// particular canCounter call.
type canCounter func(ctx context.Context, cmd string, req *Requirements, call string) (canCount int)

// cantHandlers are functions used during processQueue() that are called when
// the canCounter function returns less than the desired number of jobs. They
// represent an opportunity to try and increase available resources (eg. by
// creating new servers).
type cantHandler func(ctx context.Context, desired int, cmd string, req *Requirements, call string)

// stateUpdaters are functions used by processQueue() to update any global state
// that might have become invalid due to changes external to our own actions.
// (We make use of this in the local struct so that other implementers of
// scheduleri can embed local, use local's processQueue(), but have their own
// stateUpdater implementation.)
type stateUpdater func(ctx context.Context)

// cmdRunners are functions used by processQueue() to actually run cmds.
// (Their reason for being is the same as for canCounters.) The reservedCh
// should be sent true as soon as resources have been reserved to run the cmd,
// or sent false if something went wrong before that.
type cmdRunner func(ctx context.Context, cmd string, req *Requirements, reservedCh chan bool) error

// postProcessors are functions used by processQueue() to do something after
// a postProcess() call does work.
type postProcessor func(ctx context.Context)

// unneededCmdHandler are functions called when scheduling a cmd or completing
// the execution of a command, and we no longer need to run more of the cmd.
type unneededCmdHandler func(cmd string)

// local is our implementer of scheduleri.
type local struct {
	config            *ConfigLocal
	maxRAM            int
	maxCores          int
	ram               int
	zeroCores         int
	cores             float64
	rcount            int
	queue             *queue.Queue
	running           map[string]int
	reqCheckFunc      reqChecker
	maxMemFunc        maxResourceGetter
	maxCPUFunc        maxResourceGetter
	canCountFunc      canCounter
	cantFunc          cantHandler
	cmdNotNeededFunc  unneededCmdHandler
	postProcessFunc   postProcessor
	stateUpdateFunc   stateUpdater
	stateUpdateFreq   time.Duration
	runCmdFunc        cmdRunner
	stopAuto          chan bool
	recoveredPids     map[int]bool
	stopPidMonitoring chan struct{}
	cleanMutex        sync.RWMutex
	rcMutex           sync.RWMutex
	resourceMutex     sync.RWMutex
	runMutex          sync.RWMutex
	mutex             sync.Mutex
	rpMutex           sync.Mutex
	apMutex           sync.Mutex
	cleaned           bool
	autoProcessing    bool
	processing        bool
	recall            bool
}

// ConfigLocal represents the configuration options required by the local
// scheduler. All are required with no usable defaults.
type ConfigLocal struct {
	// Shell is the shell to use to run your commands with; 'bash' is
	// recommended.
	Shell string

	// StateUpdateFrequency is the frequency at which to re-check the queue to
	// see if anything can now run. 0 (default) is treated as 1 minute.
	StateUpdateFrequency time.Duration

	// MaxCores is the maximum number of CPU cores on the machine to use for
	// running jobs. Specifying more cores than the machine has results in using
	// as many cores as the machine has, which is also the default. Values
	// below 1 are treated as default.
	MaxCores int

	// MaxRAM is the maximum amount of machine memory to use for running jobs.
	// The unit is in MB, and defaults to all available memory. Specifying more
	// than this uses the default amount. Values below 1 are treated as default.
	MaxRAM int
}

// jobs are what we store in our queue.
type job struct {
	cmd                string
	req                *Requirements
	priority           uint8
	count              int
	scheduleDecrements int
	sync.RWMutex
}

// initialize finds out about the local machine. Compatible with amd64 archs
// only!
func (s *local) initialize(ctx context.Context, config interface{}) error {
	s.config = config.(*ConfigLocal)
	s.maxCores = runtime.NumCPU()
	if s.config.MaxCores > 0 && s.config.MaxCores < s.maxCores {
		s.maxCores = s.config.MaxCores
		if s.maxCores < 1 {
			s.maxCores = 1
		}
	}
	var err error
	s.maxRAM, err = internal.ProcMeminfoMBs()
	if err != nil {
		return err
	}
	if s.config.MaxRAM > 0 && s.config.MaxRAM < s.maxRAM {
		s.maxRAM = s.config.MaxRAM
		if s.maxRAM < 1 {
			s.maxRAM = 1
		}
	}

	// make our queue
	s.queue = queue.New(ctx, localPlace)
	s.running = make(map[string]int)

	// set our functions for use in schedule() and processQueue()
	s.reqCheckFunc = s.reqCheck
	s.maxMemFunc = s.maxMem
	s.maxCPUFunc = s.maxCPU
	s.canCountFunc = s.canCount
	s.cantFunc = s.cant
	s.runCmdFunc = s.runCmd
	s.stateUpdateFunc = s.stateUpdate
	s.stateUpdateFreq = s.config.StateUpdateFrequency
	if s.stateUpdateFreq == 0 {
		s.stateUpdateFreq = 1 * time.Minute
	}
	s.postProcessFunc = s.postProcess
	s.cmdNotNeededFunc = s.cmdNotNeeded

	s.recoveredPids = make(map[int]bool)
	s.stopPidMonitoring = make(chan struct{})

	// stopAuto is created here and not in startAutoProcessing() to avoid data
	// races with concurrent stop and start invocations
	s.stopAuto = make(chan bool)

	return err
}

// reserveTimeout achieves the aims of ReserveTimeout().
func (s *local) reserveTimeout(ctx context.Context, req *Requirements) int {
	if val, defined := req.Other["rtimeout"]; defined {
		timeout, err := strconv.Atoi(val)
		if err != nil {
			clog.Error(ctx, "Failed to convert rtimeout to integer", "error", err)
			return localReserveTimeout
		}
		return timeout
	}
	return localReserveTimeout
}

// maxQueueTime achieves the aims of MaxQueueTime().
func (s *local) maxQueueTime(req *Requirements) time.Duration {
	return infiniteQueueTime
}

// schedule achieves the aims of Schedule().
func (s *local) schedule(ctx context.Context, cmd string, req *Requirements, priority uint8, count int) error {
	if s.cleanedUp() {
		return nil
	}

	// first find out if its at all possible to ever run this cmd
	if count != 0 {
		err := s.reqCheckFunc(ctx, req)
		if err != nil {
			return err
		}
	} // else, just in case a job with these reqs somehow got through in the
	// past, allow it to be cancelled

	// priority of this cmd will be based on the given user priority, but for
	// equal priority cmds, it will be based on "size", which is the max of the
	// percentage of available memory it needs and percentage of cpus it needs.
	// A cmd that needs 100% of memory or cpu will be our highest priority
	// command, which is expressed as size 255, while one that needs 0% of
	// resources will be expressed as size 0
	maxMem := s.maxMemFunc()
	maxCPU := s.maxCPUFunc()
	percentMemNeeded := (float64(req.RAM) / float64(maxMem)) * float64(100)
	percentCPUNeeded := (req.Cores / float64(maxCPU)) * float64(100)
	percentMachineNeeded := percentMemNeeded
	if percentCPUNeeded > percentMachineNeeded {
		percentMachineNeeded = percentCPUNeeded
	}
	size := uint8(math.Round(priorityScaler * percentMachineNeeded))

	// add to the queue
	key := jobName(cmd, "n/a", false)
	data := &job{
		cmd:      cmd,
		req:      req,
		priority: priority,
		count:    count,
	}
	s.mutex.Lock()
	if s.cleanedUp() {
		return nil
	}

	item, err := s.queue.AddWithSize(ctx, key, "", data, priority, size, 0*time.Second, 30*time.Second, "") // the ttr just has to be long enough for processQueue() to process a job, not actually run the cmds
	if err != nil {
		if qerr, ok := err.(queue.Error); ok && qerr.Err == queue.ErrAlreadyExists {
			// update the job's count and item priority (only)
			j := item.Data().(*job)
			j.Lock()
			s.runMutex.RLock()
			running := s.running[key]
			s.runMutex.RUnlock()
			before := j.count
			j.count = count
			if count < running {
				j.scheduleDecrements = running - count
			} else {
				j.scheduleDecrements = 0
			}
			if j.priority != priority {
				err = s.queue.Update(ctx, key, "", j, priority, 0*time.Second, 30*time.Second)
				if err != nil {
					clog.Error(ctx, "failed to update priority", "cmd", cmd, "err", err)
				} else {
					clog.Debug(ctx, "schedule changed priority", "cmd", cmd, "before", j.priority, "now", priority)
					j.priority = priority
				}
			}
			j.Unlock()
			if count != before {
				clog.Debug(ctx, "schedule changed number needed", "cmd", cmd, "before", before, "needs", count)
			}
			if count == 0 {
				s.removeKey(ctx, key)
				clog.Debug(ctx, "schedule removed cmd", "cmd", cmd)
			}

			if !s.checkNeeded(ctx, cmd, key, count, running) {
				// bypass a pointless processQueue call
				s.mutex.Unlock()
				return nil
			}
		} else {
			s.mutex.Unlock()
			return err
		}
	} else {
		clog.Debug(ctx, "schedule added new cmd", "cmd", cmd, "needs", count, "size", size, "priority", priority)
	}
	s.mutex.Unlock()

	if count > 0 {
		s.startAutoProcessing(ctx)
	}

	// try and run the jobs in the queue
	return s.processQueue(ctx, "schedule")
}

// scheduled achieves the aims of Scheduled().
func (s *local) scheduled(ctx context.Context, cmd string) (int, error) {
	if s.cleanedUp() {
		return 0, nil
	}
	s.rcMutex.RLock()
	defer s.rcMutex.RUnlock()
	if s.queue.Stats().Items == 0 && s.rcount <= 0 {
		return 0, nil
	}

	key := jobName(cmd, "n/a", false)
	item, err := s.queue.Get(key)
	if err != nil {
		if qerr, ok := err.(queue.Error); !ok || qerr.Err != queue.ErrNotFound {
			return 0, err
		}
		return 0, nil
	}
	if item == nil {
		return 0, nil
	}

	j := item.Data().(*job)
	j.RLock()
	count := j.count
	j.RUnlock()

	return count, nil
}

// checkNeeded takes a cmd, item key, current item.Count and number of cmd
// currently running. If we do not need to run any more of this cmd, calls
// cmdNotNeededFunc(cmd).
func (s *local) checkNeeded(ctx context.Context, cmd, key string, needed, running int) bool {
	if needed <= running {
		clog.Debug(ctx, "checkNeeded not needed", "cmd", cmd, "key", key, "needed", needed, "running", running)
		s.cmdNotNeededFunc(cmd)
		return false
	}
	return true
}

// cmdCountRemaining tells you the count of cmd still needed based on what was
// supplied to schedule(), and how many we've already finished running or are
// currently running. Returns 0 if the cmd isn't known about.
func (s *local) cmdCountRemaining(cmd string) int {
	key := jobName(cmd, "n/a", false)
	item, err := s.queue.Get(key)
	if err != nil || item == nil {
		return 0
	}

	j := item.Data().(*job)
	j.RLock()
	count := j.count
	j.RUnlock()

	s.runMutex.RLock()
	running := s.running[key]
	s.runMutex.RUnlock()

	return count - running
}

// recover achieves the aims of Recover(). Here we find an untracked pid
// corresponding to the given cmd, note that the resources are in use, and
// start tracking the pid to know when it exits to release those resources.
func (s *local) recover(ctx context.Context, cmd string, req *Requirements, host *RecoveredHostDetails) error {
	processes, err := process.Processes()
	if err != nil {
		return err
	}

	cmd = cmdProcessSanitiser.Replace(cmd)
	s.rpMutex.Lock()
	defer s.rpMutex.Unlock()
	for _, p := range processes {
		thisCmd, err := p.Cmdline()
		if err != nil {
			// likely the process stopped existing between the call to
			// Processes() and now, just ignore this one
			continue
		}
		if cmd == thisCmd {
			pid := int(p.Pid)
			if s.recoveredPids[pid] {
				continue
			}

			s.resourceMutex.Lock()
			s.ram += req.RAM
			if req.Cores == 0 {
				s.zeroCores++
			} else {
				s.cores += req.Cores
			}
			s.resourceMutex.Unlock()

			go func() {
				defer internal.LogPanic(ctx, "recover", true)

				// periodically check on this pid; when it has exited, update
				// our resource usage
				ticker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-ticker.C:
						process, errf := os.FindProcess(pid)
						alive := true
						if errf != nil {
							alive = false
						} else {
							errs := process.Signal(syscall.Signal(0))
							if errs != nil {
								alive = false
							}
						}

						if !alive {
							ticker.Stop()

							s.resourceMutex.Lock()
							s.ram -= req.RAM
							if req.Cores == 0 {
								s.zeroCores--
							} else {
								s.cores -= req.Cores
							}
							s.resourceMutex.Unlock()

							errp := s.processQueue(ctx, "recover")
							if errp != nil {
								clog.Error(ctx, "processQueue call after recovery failed", "err", errp)
							}

							return
						}
					case <-s.stopPidMonitoring:
						ticker.Stop()
						return
					}
				}
			}()

			s.recoveredPids[pid] = true
			break
		}
	}
	return nil
}

// reqCheck gives an ErrImpossible if the given Requirements can not be met.
func (s *local) reqCheck(ctx context.Context, req *Requirements) error {
	if req.RAM > s.maxRAM || int(math.Ceil(req.Cores)) > s.maxCores {
		return Error{"local", "schedule", ErrImpossible}
	}
	return nil
}

// maxMem returns the maximum memory available on the machine in MB.
func (s *local) maxMem() int {
	return s.maxRAM
}

// maxCPU returns the total number of CPU cores available on the machine.
func (s *local) maxCPU() int {
	return s.maxCores
}

// removeKey removes a key from the queue, for when there are no more jobs for
// that key. If this results in an empty queue, stops autoProcessing.
func (s *local) removeKey(ctx context.Context, key string) {
	err := s.queue.Remove(ctx, key)
	if err != nil {
		qerr, ok := err.(queue.Error)

		if ok && qerr.Err == queue.ErrQueueClosed {
			return
		}

		// warn unless we've already removed this key
		if !ok || qerr.Err != queue.ErrNotFound {
			clog.Warn(ctx, "processQueue item removal failed", "err", err)
		}
	}
	if s.queue.Stats().Items == 0 {
		s.stopAutoProcessing()
	}
}

// processQueue goes through the jobs in the queue by size, sees if it's
// possible to run any, does so if it is, otherwise returns the jobs to the
// queue.
func (s *local) processQueue(ctx context.Context, reason string) error {
	// only process the queue once at a time; other calls to this function
	// will return immeditely but cause us to recall ourselves when we
	// complete
	s.mutex.Lock()
	if s.cleanedUp() {
		s.mutex.Unlock()
		return nil
	}
	if s.processing {
		s.recall = true
		s.mutex.Unlock()
		return nil
	}
	s.processing = true
	s.mutex.Unlock()
	clog.Debug(ctx, "processQueue starting", "reason", reason)

	// now perform any global state update needed by the scheduler
	s.stateUpdateFunc(ctx)

	stats := s.queue.Stats()
	toRelease := make([]string, 0, stats.Items)
	defer func() {
		for _, key := range toRelease {
			errr := s.queue.Release(ctx, key)
			if errr != nil {
				if qerr, ok := errr.(queue.Error); !ok || (qerr.Err != queue.ErrNotFound && qerr.Err != queue.ErrQueueClosed) {
					clog.Warn(ctx, "processQueue item release failed", "err", errr)
				}
			}
		}

		s.postProcessFunc(ctx)

		s.mutex.Lock()
		s.processing = false
		recall := s.recall
		s.recall = false
		s.mutex.Unlock()
		if recall {
			go func() {
				defer internal.LogPanic(ctx, "processQueue recall", true)
				errp := s.processQueue(ctx, "recall")
				if errp != nil {
					clog.Warn(ctx, "processQueue recall failed", "err", errp)
				}
			}()
		}

		clog.Debug(ctx, "processQueue ending")
	}()

	// go through the jobs largest to smallest (standard bin packing approach)
	for {
		item, err := s.queue.Reserve("", 0)
		if err != nil {
			if qerr, ok := err.(queue.Error); ok && (qerr.Err == queue.ErrNothingReady || qerr.Err == queue.ErrQueueClosed) {
				return nil
			}
			return err
		}
		key := item.Key
		j := item.Data().(*job)
		j.RLock()
		cmd := j.cmd
		req := j.req
		count := j.count

		s.runMutex.Lock()
		running := s.running[key]
		clog.Debug(ctx, "processQueue binpacking", "needs", count, "current", running, "cmd", cmd)
		if count == 0 && running == 0 {
			// a cancellation has come in, and somehow we didn't remove this
			// from the queue; do so now
			clog.Debug(ctx, "processQueue cancelling", "cmd", cmd)
			s.removeKey(ctx, key)
			s.runMutex.Unlock()
			j.RUnlock()
			continue
		}
		toRelease = append(toRelease, key)
		shouldCount := count - running
		if shouldCount <= 0 {
			// we're already running everything for this job, try the next
			// largest cmd
			s.runMutex.Unlock()
			j.RUnlock()
			continue
		}

		// now see if there's remaining capacity to run the job
		call := logext.RandId(8)
		ctx = clog.ContextWithCallValue(ctx, call)

		canCount := s.canCountFunc(ctx, cmd, req, call)
		clog.Debug(ctx, "processQueue canCount", "can", canCount, "running", running, "should", shouldCount)
		if canCount > shouldCount {
			canCount = shouldCount
		}

		if canCount < shouldCount {
			s.cantFunc(ctx, shouldCount-canCount, cmd, req, call)
		}

		if canCount <= 0 {
			// try and fill any "gaps" (spare memory/ cpu) by seeing if a cmd
			// with lesser resource requirements can be run
			s.runMutex.Unlock()
			j.RUnlock()
			continue
		}

		// start running what we can
		clog.Debug(ctx, "processQueue runCmdFunc", "count", canCount)
		reserved := make(chan bool, canCount)
		for i := 0; i < canCount; i++ {
			s.running[key]++
			s.checkNeeded(ctx, cmd, key, count, s.running[key])

			go func() {
				defer internal.LogPanic(ctx, "processQueue runCmd loop", true)

				clog.Debug(ctx, "will run cmd", "cmd", cmd, "call", call)
				err := s.runCmdFunc(ctx, cmd, req, reserved)
				clog.Debug(ctx, "ran cmd", "cmd", cmd, "call", call)

				j.Lock()
				s.runMutex.Lock()
				s.running[key]--
				if s.running[key] <= 0 {
					delete(s.running, key)
				}

				// decrement j.count here if we didn't already decrement it
				// during a schedule() call
				if err == nil {
					if j.scheduleDecrements > 0 {
						j.scheduleDecrements--
					} else {
						j.count--
					}
					jCount := j.count
					if jCount <= 0 {
						s.removeKey(ctx, key)
					}
				}
				s.runMutex.Unlock()
				j.Unlock()

				if err != nil {
					// users are notified of relevant errors during runCmd; here
					// we just debug log everything
					clog.Debug(ctx, "runCmd error", "err", err)
				}

				err = s.processQueue(ctx, "after runCmd")
				if err != nil {
					clog.Error(ctx, "processQueue recall failed", "err", err)
				}
			}()
		}
		s.runMutex.Unlock()
		j.RUnlock()

		clog.Debug(ctx, "processQueue runCmdFunc loop complete")

		// before looping again, wait for all the above runCmdFuncs to at least
		// get as far as reserving their resources, so subsequent calls to
		// canCountFunc will be accurate. Also try and ensure that if something
		// goes wrong sending on the reserved channel, we don't get stuck here
		ch := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			for i := 0; i < canCount; i++ {
				<-reserved
			}
			done <- true
			ch <- true
		}()
		go func() {
			select {
			case <-time.After(1 * time.Minute):
				ch <- false
			case <-done:
				return
			}
		}()
		sentAll := <-ch
		if !sentAll {
			clog.Warn(ctx, "processQueue failed to reserve all resources")
		}

		// keep looping, in case any smaller job can also be run
	}
}

// canCount tells you how many jobs with the given RAM and core requirements it
// is possible to run, given remaining resources.
func (s *local) canCount(ctx context.Context, cmd string, req *Requirements, call string) int {
	s.resourceMutex.RLock()
	defer s.resourceMutex.RUnlock()

	// we don't do any actual checking of current resources on the machine, but
	// instead rely on our simple tracking based on how many cores and RAM prior
	// cmds were /supposed/ to use. This could be bad for misbehaving cmds that
	// use too much RAM, but we will end up killing cmds that do this, so it
	// shouldn't be too much of an issue.
	canCount := int(math.Floor(float64(s.maxRAM-s.ram) / float64(req.RAM)))
	if canCount < 0 {
		clog.Warn(ctx, "negative canCount", "can", canCount, "maxRam", s.maxRAM, "ram", s.ram, "reqRam", req.RAM)
		canCount = 0
	}
	if canCount >= 1 {
		var canCount2 int
		if req.Cores == 0 {
			// rather than allow an infinite or very large number of cmds to run
			// on this machine, because there are still real limits on the
			// number of processes we can run at once before things start
			// falling over, we only allow double the actual core count of zero
			// core things to run (on top of up to actual core count of non-zero
			// core things)
			canCount2 = s.maxCores*internal.ZeroCoreMultiplier - s.zeroCores
		} else {
			canCount2 = int(math.Floor(internal.FloatSubtract(float64(s.maxCores), s.cores) / req.Cores))
		}
		if canCount2 < canCount {
			canCount = canCount2
			if canCount < 0 {
				clog.Warn(ctx, "negative canCount", "can", canCount, "maxCores", s.maxCores, "cores",
					s.cores, "zeroCores", s.zeroCores, "reqCores", req.Cores)
				canCount = 0
			}
		}
	}
	return canCount
}

// cant is our cantFunc, which in the local case does nothing, since we can't
// increase available resources.
func (s *local) cant(ctx context.Context, desired int, cmd string, req *Requirements, call string) {}

// runCmd runs the command, kills it if it goes much over RAM or time limits.
// NB: we only return an error if we can't start the cmd, not if the command
// fails (schedule() only guarantees that the cmds are run count times, not that
// they run /successful/ that many times).
func (s *local) runCmd(ctx context.Context, cmd string, req *Requirements, reservedCh chan bool) error {
	sr := func(v bool) {
		// *** reservedCh is buffered and sending on it should never
		// block, but somehow we have gotten stuck here before; make
		// sure we don't get stuck on this send
		ch := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			reservedCh <- v
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
			clog.Warn(ctx, "failed to send on reservedCh")
		}
	}

	ec := exec.Command(s.config.Shell, "-c", cmd) // #nosec
	ec.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	err := ec.Start()
	if err != nil {
		clog.Error(ctx, "runCmd start", "cmd", cmd, "err", err)
		sr(false)
		return err
	}

	s.rcMutex.Lock()
	s.rcount++
	s.rcMutex.Unlock()

	s.resourceMutex.Lock()
	s.ram += req.RAM
	if req.Cores == 0 {
		s.zeroCores++
	} else {
		s.cores += req.Cores
	}
	sr(true)
	s.resourceMutex.Unlock()

	//*** set up monitoring of RAM and time usage and kill if >> than
	// req.RAM or req.Time

	err = ec.Wait()
	if err != nil {
		clog.Error(ctx, "runCmd wait", "cmd", cmd, "err", err)
	}

	s.rcMutex.Lock()
	s.rcount--
	if s.rcount < 0 {
		s.rcount = 0
	}
	s.rcMutex.Unlock()

	s.resourceMutex.Lock()
	s.ram -= req.RAM
	if req.Cores == 0 {
		s.zeroCores--
	} else {
		s.cores -= req.Cores
	}
	s.resourceMutex.Unlock()

	return nil // do not return error running the command
}

// stateUpdate in the local scheduler is a no-op, since there currently isn't
// any state out of our control we worry about.
func (s *local) stateUpdate(ctx context.Context) {}

// postProcess in the local scheduler is a no-op, since there currently isn't
// anything that needs to be done after a postProcess() call.
func (s *local) postProcess(ctx context.Context) {}

// cmdNotNeeded in the local scheduler is a no-op, since there currently isn't
// anything that needs to be done when a cmd is no longer needed.
func (s *local) cmdNotNeeded(cmd string) {}

// startAutoProcessing begins periodic running of processQueue(). Normally
// processQueue is only called when cmds are added or complete. Calling it
// periodically as well means we are responsive to external events freeing up
// resources.
func (s *local) startAutoProcessing(ctx context.Context) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.cleanedUp() {
		return
	}
	s.apMutex.Lock()
	defer s.apMutex.Unlock()
	if s.autoProcessing {
		return
	}

	go func() {
		defer internal.LogPanic(ctx, "auto processQueue", false)

		ticker := time.NewTicker(s.stateUpdateFreq)
		for {
			select {
			case <-ticker.C:
				// processQueue can end up calling stopAutoProcessing which
				// will wait on the read of stopAuto below, but we won't read
				// it until this case completes, so call processQueue in a go
				// routine to complete the case ~instantly
				go func() {
					err := s.processQueue(ctx, "auto")
					if err != nil {
						clog.Error(ctx, "Automated processQueue call failed", "err", err)
					}
				}()
				continue
			case <-s.stopAuto:
				ticker.Stop()
				return
			}
		}
	}()

	s.autoProcessing = true
}

// stopAutoProcessing turns off the periodic processQueue() calls initiated by
// startAutoProcessing().
func (s *local) stopAutoProcessing() {
	s.apMutex.Lock()
	defer s.apMutex.Unlock()
	if !s.autoProcessing {
		return
	}

	s.stopAuto <- true

	s.autoProcessing = false
}

// busy returns true if there's anything in our queue or we are still running
// any cmd.
func (s *local) busy(ctx context.Context) bool {
	if s.cleanedUp() {
		return false
	}
	s.rcMutex.RLock()
	defer s.rcMutex.RUnlock()
	if s.queue.Stats().Items == 0 && s.rcount <= 0 {
		return false
	}
	return true
}

// hostToID always returns an empty string, since we're not in the cloud.
func (s *local) hostToID(host string) string {
	return ""
}

// localHost implements the Host interface.
type localHost struct {
	logger log15.Logger
	shell  string
}

// RunCmd runs the given command on localhost, optionally in the background.
// You get the command's STDOUT and STDERR as strings.
func (l *localHost) RunCmd(ctx context.Context, cmd string, background bool) (stdout, stderr string, err error) {
	done := make(chan error, 1)
	outCh := make(chan string, 1)
	errCh := make(chan string, 1)

	go func() {
		defer internal.LogPanic(ctx, "localHost RunCmd", false)

		if background {
			cmd = "sh -c 'nohup " + cmd + " > /dev/null 2>&1 &'"
		}

		ec := exec.CommandContext(ctx, l.shell, "-c", cmd) // #nosec

		stdoutp, errs := ec.StdoutPipe()
		if errs != nil {
			done <- errs

			return
		}

		stderrp, errs := ec.StderrPipe()
		if errs != nil {
			done <- errs

			return
		}

		if errs := ec.Start(); errs != nil {
			done <- errs

			return
		}

		stdout, erro := io.ReadAll(stdoutp)
		stderr, erre := io.ReadAll(stderrp)

		if errw := ec.Wait(); errw != nil {
			done <- errw

			return
		}

		if erro == nil && len(stdout) > 0 {
			outCh <- string(stdout)
		} else {
			outCh <- ""
		}
		if erre == nil && len(stderr) > 0 {
			errCh <- string(stderr)
		} else {
			errCh <- ""
		}
		done <- nil
	}()

	err = <-done
	if err == nil {
		stdout = <-outCh
		stderr = <-errCh
	}

	return stdout, stderr, err
}

// getHost returns an implementation of the Host interface that can be used
// to run commands on localhost.
func (s *local) getHost(host string) (Host, bool) {
	return &localHost{shell: s.config.Shell}, true
}

// setMessageCallBack does nothing at the moment, since we don't generate any
// messages for the user.
func (s *local) setMessageCallBack(ctx context.Context, cb MessageCallBack) {}

// setBadServerCallBack does nothing, since we're not a cloud-based scheduler.
func (s *local) setBadServerCallBack(ctx context.Context, cb BadServerCallBack) {}

// cleanup destroys our internal queue.
func (s *local) cleanup(ctx context.Context) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	s.stopAutoProcessing()
	s.cleanMutex.Lock()
	defer s.cleanMutex.Unlock()
	close(s.stopPidMonitoring)
	s.cleaned = true
	err := s.queue.Destroy()
	if err != nil {
		clog.Warn(ctx, "local scheduler cleanup failed", "err", err)
	}
}

// cleanedUp returns true if cleanup() has been called
func (s *local) cleanedUp() bool {
	s.cleanMutex.RLock()
	defer s.cleanMutex.RUnlock()
	return s.cleaned
}
