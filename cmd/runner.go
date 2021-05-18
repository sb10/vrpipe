// Copyright © 2016-2018, 2021 Genome Research Limited
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

package cmd

import (
	"context"
	"fmt"
	"log/syslog"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/inconshreveable/log15"
	"github.com/kardianos/osext"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wr/clog"
)

// options for this cmd
var schedgrp string
var timeoutintRunner int
var reserveint int
var rserver string
var rdomain string
var maxtime int
var logToSyslog bool

// runnerCmd represents the runner command
var runnerCmd = &cobra.Command{
	Use:   "runner",
	Short: "Run queued commands",
	Long: `A runner runs commands that were queued by the add or setup commands.

You won't normally run this yourself directly - "wr manager" spawns these as
needed.

A runner will pick up a queued command and run it. Once that cmd completes, the
runner will pick up another and so on. Once max_time has been used (or would be
used based on the expected time to complete of the next queued command), the
runner stops picking up new commands and exits instead; max_time does not cause
the runner to kill itself if the cmd it is running takes longer than max_time to
complete.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		if runtime.NumCPU() == 1 {
			// we might lock up with only 1 proc if we mount
			runtime.GOMAXPROCS(2)
		}

		if logToSyslog {
			handler, err := log15.SyslogHandler(syslog.LOG_USER, "wrrunner", log15.LogfmtFormat())
			if err != nil {
				warn("failed to set up syslog logging: %s", err)
			} else {
				clog.ToHandlerAtLevel(handler, "info")
			}
		}

		info("wr runner started for scheduler group '%s'", schedgrp)

		// the server receive timeout must be greater than the time we'll wait
		// to Reserve()
		if timeoutintRunner < (reserveint + 5) {
			timeoutintRunner = reserveint + 5
		}
		timeout := time.Duration(timeoutintRunner) * time.Second
		rtimeout := time.Duration(reserveint) * time.Second

		jobqueue.AppName = "wr"

		token, err := token()
		if err != nil {
			die("%s", err)
		}
		jq, err := jobqueue.Connect(rserver, caFile, rdomain, token, timeout)
		if err != nil {
			die("%s", err)
		}
		defer func() {
			err = jq.Disconnect()
			if err != nil {
				warn("Disconnecting from the server failed: %s", err)
			}
		}()

		// in case any job we execute has a Cmd that calls `wr add`, we will
		// override their environment to make that call work
		var envOverrides []string
		var exePath string
		if rserver != "" {
			hostPort := strings.Split(rserver, ":")
			if len(hostPort) == 2 {
				envOverrides = append(envOverrides, "WR_MANAGERHOST="+hostPort[0])
				envOverrides = append(envOverrides, "WR_MANAGERPORT="+hostPort[1])
			}
			envOverrides = append(envOverrides, "WR_MANAGERCERTDOMAIN="+rdomain)

			// later we will add our own wr exe to the path if not there
			exe, err := osext.Executable()
			if err != nil {
				die("%s", err)
			}
			exePath = filepath.Dir(exe)
		}

		// we'll stop the below loop before using up too much time
		var endTime time.Time
		if maxtime > 0 {
			endTime = time.Now().Add(time.Duration(maxtime) * time.Minute)
		} else {
			endTime = time.Now().AddDate(1, 0, 0) // default to allowing us a year to run
		}

		// loop, reserving and running commands from the queue, until there
		// aren't any more commands in the queue
		numrun := 0
		exitReason := fmt.Sprintf("there are no more commands in scheduler group '%s'", schedgrp)
		var jobTime time.Duration
		for {
			// see if we have enough time to run a new job before we should
			// exit
			if time.Now().Add(jobTime).After(endTime) {
				exitReason = "we're about to hit our maximum time limit"
				break
			}

			var job *jobqueue.Job
			var err error
			if schedgrp == "" {
				job, err = jq.Reserve(rtimeout)
			} else {
				job, err = jq.ReserveScheduled(rtimeout, schedgrp)
			}

			if err != nil {
				die("%s", err)
			}
			if job == nil {
				break
			}

			if job.Requirements.Time != jobTime {
				// confirm we have enough time left to run this
				jobTime = job.Requirements.Time
				if time.Now().Add(jobTime).After(endTime) {
					err = jq.Release(job, nil, "not enough time to run")
					if err != nil {
						// oh well?
						warn("job release after running out of time failed: %s", err)
					}
					exitReason = "we're about to hit our maximum time limit"
					break
				}
			}

			// actually run the cmd
			if len(envOverrides) > 0 {
				// add exePath to this job's PATH
				env, erre := job.Env()
				if erre != nil {
					err = jq.Release(job, nil, "failed to read job's Env")
					if err != nil {
						warn("job release after Env() fail: %s", erre)
					}
					exitReason = "Env failed"
					break
				}
				for _, envvar := range env {
					pair := strings.Split(envvar, "=")
					if pair[0] == "PATH" {
						if !strings.Contains(pair[1], exePath) {
							envOverrides = append(envOverrides, envvar+":"+exePath)
						}
						break
					}
				}

				err = job.EnvAddOverride(envOverrides)
				if err != nil {
					err = jq.Release(job, nil, "failed to add env var overrides")
					if err != nil {
						// oh well?
						warn("job release after envaddoverride fail: %s", err)
					}
					exitReason = "EnvAddOverride failed"
					break
				}
			}

			info("will start executing [%s]", job.Cmd)
			err = jq.Execute(ctx, job, config.RunnerExecShell)
			numrun++
			if err != nil {
				warn("%s", err)
				if jqerr, ok := err.(jobqueue.Error); ok {
					if strings.Contains(jqerr.Err, jobqueue.FailReasonSignal) {
						exitReason = "we received a signal to stop"
						break
					} else if strings.Contains(jqerr.Err, jobqueue.ErrStopReserving) {
						exitReason = "we reconnected to a new server"
						break
					}
				}
			} else {
				info("command [%s] ran OK (exit code %d)", job.Cmd, job.Exitcode)
			}
		}

		info("wr runner exiting, having run %d commands, because %s", numrun, exitReason)
	},
}

func init() {
	ctx := context.Background()
	RootCmd.AddCommand(runnerCmd)

	// flags specific to this sub-command
	runnerCmd.Flags().StringVarP(&schedgrp, "scheduler_group", "s", "", "specify the scheduler group to limit which commands can be acted on")
	runnerCmd.Flags().IntVar(&timeoutintRunner, "timeout", 30, "how long (seconds) to wait to get a reply from 'wr manager'")
	runnerCmd.Flags().IntVarP(&reserveint, "reserve_timeout", "r", 2, "how long (seconds) to wait for there to be a command in the queue, before exiting")
	runnerCmd.Flags().IntVarP(&maxtime, "max_time", "m", 0, "maximum time (minutes) to run for before exiting; 0 means unlimited")
	runnerCmd.Flags().StringVar(&rserver, "server", internal.DefaultServer(ctx), "ip:port of wr manager")
	runnerCmd.Flags().StringVar(&rdomain, "domain", internal.DefaultConfig(ctx).ManagerCertDomain,
		"domain the manager's cert is valid for")
	runnerCmd.Flags().BoolVar(&logToSyslog, "debug", false, "enable logging to syslog")
}
