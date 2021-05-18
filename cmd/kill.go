// Copyright © 2018 Genome Research Limited
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
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/spf13/cobra"
)

// options for this cmd
var confirmDead bool
var cmdAge string

// killCmd represents the kill command
var killCmd = &cobra.Command{
	Use:   "kill",
	Short: "Kill running commands",
	Long: `You can kill commands you've previously added with "wr add" that
are currently running using this command.

After killing commands, there will be a delay before the commands "realise" they
have been killed and actually stop running. At that point they will become
buried and you can "wr remove" them if desired.

Specify one of the flags -f, -l, -i or -a to choose which commands you want to
remove. Amongst those, only running jobs will be affected. If the --confirmdead
option is specified, only "lost contact" jobs will be affected.

-i is the report group (-i) you supplied to "wr add" when you added the job(s)
you want to now kill. Combining with -z lets you kill jobs in multiple report
groups, assuming you have arranged that related groups share some substring.
Alternatively -y lets you specify -i as the internal job id reported during
"wr status".

The file to provide -f is in the format taken by "wr add".

In -f and -l mode you must provide the cwd the commands were set to run in, if
CwdMatters (and must NOT be provided otherwise). Likewise provide the mounts
options that was used when the command was added, if any. You can do this by
using the -c and --mounts/--mounts_json options in -l mode, or by providing the
same file you gave to "wr add" in -f mode.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		set := countGetJobArgs()
		if set > 1 {
			die("-f, -i, -l and -a are mutually exclusive; only specify one of them")
		}
		if set == 0 {
			die("1 of -f, -i, -l or -a is required")
		}

		timeout := time.Duration(timeoutint) * time.Second
		jq := connect(timeout)
		var err error
		defer func() {
			err = jq.Disconnect()
			if err != nil {
				warn("Disconnecting from the server failed: %s", err)
			}
		}()

		jstate := jobqueue.JobStateRunning
		if confirmDead {
			jstate = jobqueue.JobStateLost
		}
		jobs := getJobs(ctx, jq, jstate, cmdAll, 0, false, false)

		if len(jobs) == 0 {
			die("No matching jobs found")
		}

		var age time.Duration
		if cmdAge != "" {
			age, err = time.ParseDuration(cmdAge)
			if err != nil {
				die("--age was not specified correctly: %s", err)
			}
		}

		if age != 0 {
			var oldJobs []*jobqueue.Job
			for _, job := range jobs {
				var thisAge time.Duration
				if confirmDead {
					thisAge = time.Since(job.EndTime)
				} else {
					thisAge = job.WallTime()
				}

				if thisAge >= age {
					oldJobs = append(oldJobs, job)
				}
			}

			jobs = oldJobs
		}

		if len(jobs) == 0 {
			die("No matching jobs were older than %s", cmdAge)
		}

		jes := jobsToJobEssenses(jobs)
		killed, err := jq.Kill(jes)
		if err != nil {
			die("failed to kill desired jobs: %s", err)
		}
		info("Initiated the termination of %d running commands (out of %d eligible)", killed, len(jobs))
	},
}

func init() {
	RootCmd.AddCommand(killCmd)

	// flags specific to this sub-command
	killCmd.Flags().BoolVarP(&cmdAll, "all", "a", false, "kill all running jobs")
	killCmd.Flags().StringVarP(&cmdFileStatus, "file", "f", "", "file containing commands you want to kill; - means read from STDIN")
	killCmd.Flags().StringVarP(&cmdIDStatus, "identifier", "i", "", "identifier of the commands you want to kill")
	killCmd.Flags().BoolVarP(&cmdIDIsSubStr, "search", "z", false, "treat -i as a substring to match against all report groups")
	killCmd.Flags().BoolVarP(&cmdIDIsInternal, "internal", "y", false, "treat -i as an internal job id")
	killCmd.Flags().StringVarP(&cmdLine, "cmdline", "l", "", "a command line you want to kill")
	killCmd.Flags().StringVarP(&cmdCwd, "cwd", "c", "", "working dir that the command(s) specified by -l or -f were set to run in")
	killCmd.Flags().StringVarP(&mountJSON, "mount_json", "j", "", "mounts that the command(s) specified by -l or -f were set to use (JSON format)")
	killCmd.Flags().StringVar(&mountSimple, "mounts", "", "mounts that the command(s) specified by -l or -f were set to use (simple format)")
	killCmd.Flags().BoolVar(&confirmDead, "confirmdead", false, "only confirm that lost contact jobs are dead")
	killCmd.Flags().StringVar(&cmdAge, "age", "", "only kill jobs that have been running longer than this (or in --confirmdead mode, that have been lost longer than this). [specify units such as m for minutes or h for hours]")

	killCmd.Flags().IntVar(&timeoutint, "timeout", 120, "how long (seconds) to wait to get a reply from 'wr manager'")
}
