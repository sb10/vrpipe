// Copyright © 2018, 2021 Genome Research Limited
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

var onlyBuried bool

// removeCmd represents the remove command
var removeCmd = &cobra.Command{
	Use:   "remove",
	Short: "Remove added commands",
	Long: `You can remove commands you've previously added with "wr add" that
are currently incomplete and not running using this command.

For use when you've made a mistake when specifying the command and it will never
work. If you want to remove commands that are currently running you will need to
"wr kill" them first.

Specify one of the flags -f, -l, -i or -a to choose which commands you want to
remove. Amongst those, only currently incomplete, non-running jobs will be
affected.

-i is the report group (-i) you supplied to "wr add" when you added the job(s)
you want to now remove. Combining with -z lets you remove jobs in multiple
report groups, assuming you have arranged that related groups share some
substring. Alternatively -y lets you specify -i as the internal job id reported
during "wr status".

The file to provide -f is in the format taken by "wr add".

In -f and -l mode you must provide the cwd the commands were set to run in, if
CwdMatters (and must NOT be provided otherwise). Likewise provide the mounts
options that was used when the command was added, if any. You can do this by
using the -c and --mounts/--mounts_json options in -l mode, or by providing the
same file you gave to "wr add" in -f mode.

Note that you can't remove a job that has other jobs depending upon it, unless
you also remove those jobs at the same time. If there is a mistake in the
command line of a job with dependants, you can either:
1) use the -f option to remove all the incomplete jobs you added, fix the
   command line of the bad job by editing the -f file, then use "wr add" to add
   all the jobs back again with the corrected -f file
-or-
2) use the "wr mod" command to modify the command line of the bad job while
   leaving those jobs that are dependant upon it intact.`,
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

		cmdState := jobqueue.JobStateDeletable
		desc := "incomplete, non-running"
		if onlyBuried {
			cmdState = jobqueue.JobStateBuried
			desc = "buried"
		}

		jobs := getJobs(ctx, jq, cmdState, cmdAll, 0, false, false)

		if len(jobs) == 0 {
			die("No matching jobs found")
		}

		jes := jobsToJobEssenses(jobs)
		removed, err := jq.Delete(jes)
		if err != nil {
			die("failed to remove desired jobs: %s", err)
		}
		info("Removed %d %s commands (out of %d eligible)", removed, desc, len(jobs))
	},
}

func init() {
	RootCmd.AddCommand(removeCmd)

	// flags specific to this sub-command
	removeCmd.Flags().BoolVarP(&cmdAll, "all", "a", false, "remove all incomplete, non-running jobs")
	removeCmd.Flags().StringVarP(&cmdFileStatus, "file", "f", "", "file containing commands you want to remove; - means read from STDIN")
	removeCmd.Flags().StringVarP(&cmdIDStatus, "identifier", "i", "", "identifier of the commands you want to remove")
	removeCmd.Flags().BoolVarP(&cmdIDIsSubStr, "search", "z", false, "treat -i as a substring to match against all report groups")
	removeCmd.Flags().BoolVarP(&cmdIDIsInternal, "internal", "y", false, "treat -i as an internal job id")
	removeCmd.Flags().StringVarP(&cmdLine, "cmdline", "l", "", "a command line you want to remove")
	removeCmd.Flags().StringVarP(&cmdCwd, "cwd", "c", "", "working dir that the command(s) specified by -l or -f were set to run in")
	removeCmd.Flags().StringVarP(&mountJSON, "mount_json", "j", "", "mounts that the command(s) specified by -l or -f were set to use (JSON format)")
	removeCmd.Flags().StringVar(&mountSimple, "mounts", "", "mounts that the command(s) specified by -l or -f were set to use (simple format)")
	removeCmd.Flags().BoolVarP(&onlyBuried, "buried", "b", false, "only delete jobs that are currently buried")

	removeCmd.Flags().IntVar(&timeoutint, "timeout", 120, "how long (seconds) to wait to get a reply from 'wr manager'")
}
