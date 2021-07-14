// Copyright © 2019, 2021 Genome Research Limited
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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/spf13/cobra"
)

// options for this cmd
var cmdCwdMattersUnset bool
var cmdChangeHomeUnset bool
var cmdCloudSharedDiskUnset bool

const nothingBehaviour = `[{"Nothing":true}]`

// modCmd represents the mod command
var modCmd *cobra.Command = &cobra.Command{
	Use:   "mod",
	Short: "Modify commands previously added to the queue",
	Long: `Modify commands previously added to the queue.

If you have already added a command to the queue with "wr add", but then decide
you want to change something about it, assuming it isn't currently running you
could use "wr remove" to remove the command and then add a new corrected command
back.

However, if the command has other commands depending upon it, you won't be able
to remove it. You'll have to remove all the commands and then add them back
afterwards.

Instead of having to do that, this "mod" command makes it easier to change any
aspect of a command without affecting or triggering its dependencies. The only
aspects of a command you can't modify are its report group identifier,
dependency groups and bsub mode.

If you want to modify commands that are currently running you will need to
"wr kill" them first.

Specify one of the flags -i or -a to choose which commands you want to
modify. Amongst those, only currently incomplete, non-running command will be
affected.

-i is the report group (-i) you supplied to "wr add" when you added the
command(s) you want to now modify. Combining with -z lets you modify commands in
multiple report groups, assuming you have arranged that related groups share
some substring. Alternatively -y lets you specify -i as the internal job id
reported during "wr status".

Having identified the command(s) to modify, provide any of "wr add"'s options
(except for -f, -i, --rerun, --dep_grps and --bsub) to change that aspect of the
command. If the boolean options --cwd_matters, --change_home or --cloud_shared
were used during "add", these can be turned off with the special options
--unset_cwd_matters, --unset_change_home and --unset_cloud_shared respectively.
To turn off other options, supply an empty string as the value.

To turn off a behaviour, supply an empty string, eg. --on_exit "".

To change the command line of a command, you must have selected only a single
command (eg. by specifying an internal job id with -i -y). You can then use
--cmdline to specify the new command. You can't specify the command of another
command in the queue or that has previously completed; those mod requests will
be silently ignored.

Because modifying a command may change its internal id, a mapping of old to
new internal ids is printed.`,
	Run: func(cobraCmd *cobra.Command, args []string) {
		ctx := context.Background()
		// check the command line options
		if cmdAll && cmdIDStatus != "" {
			die("-a and -i are mutually exclusive")
		}
		if cmdAll && cmdLine != "" {
			die("-a is not compatible with --cmdline")
		}
		if cmdIDStatus == "" && (cmdIDIsSubStr || cmdIDIsInternal) {
			die("-z and -y require -i")
		}
		if !cmdAll && cmdIDStatus == "" {
			die("one of -i or -a is required")
		}

		// we call getJobs() later, which finds jobs based on -f and -l, but
		// we don't want that
		cmdFile = ""
		var newCmdLine string
		if cmdLine != "" {
			newCmdLine = cmdLine
			cmdLine = ""
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

		// get the job(s) user wishes to modify
		jobs := getJobs(ctx, jq, jobqueue.JobStateDeletable, cmdAll, 0, false, false)

		if len(jobs) == 0 {
			die("No matching jobs found")
		}

		if len(jobs) > 1 && newCmdLine != "" {
			die("%d jobs matched your query, but -l can only be used with 1 job", len(jobs))
		}

		// note what properties of the job(s) the user wishes to modify
		jm := jobqueue.NewJobModifer()

		if newCmdLine != "" {
			jm.SetCmd(newCmdLine)
		}
		if cobraCmd.Flags().Changed("limit_grps") {
			jm.SetLimitGroups(strings.Split(cmdLimitGroups, ","))
		}

		// *** implementing dep_grps modification is complex; not done for now
		// if cobraCmd.Flags().Changed("dep_grps") {
		// 	jm.SetDepGroups(strings.Split(cmdDepGroups, ","))
		// }

		if cobraCmd.Flags().Changed("cwd") {
			jm.SetCwd(cmdCwd)
		}
		if cmdCwdMatters {
			jm.SetCwdMatters(true)
		} else if cmdCwdMattersUnset {
			jm.SetCwdMatters(false)
		}
		if cmdChangeHome {
			jm.SetChangeHome(true)
		} else if cmdChangeHomeUnset {
			jm.SetChangeHome(false)
		}
		if cobraCmd.Flags().Changed("req_grp") {
			jm.SetReqGroup(reqGroup)
		}

		req := &jqs.Requirements{}
		var setReq bool
		if cobraCmd.Flags().Changed("memory") {
			mb, errt := bytefmt.ToMegabytes(cmdMem)
			if errt != nil {
				die("--memory was not specified correctly: %s", errt)
			}
			req.RAM = int(mb)
			setReq = true
		}
		if cobraCmd.Flags().Changed("time") {
			t, errp := time.ParseDuration(cmdTime)
			if errp != nil {
				die("--time was not specified correctly: %s", errp)
			}
			req.Time = t
			setReq = true
		}
		if cobraCmd.Flags().Changed("cpus") {
			req.Cores = cmdCPUs
			req.CoresSet = true
			setReq = true
		}
		if cobraCmd.Flags().Changed("disk") {
			req.Disk = cmdDisk
			req.DiskSet = true
			setReq = true
		}

		other := make(map[string]string)
		var otherSet bool
		if cobraCmd.Flags().Changed("cloud_os") {
			other["cloud_os"] = cmdOsPrefix
		}
		if cobraCmd.Flags().Changed("cloud_username") {
			other["cloud_user"] = cmdOsUsername
		}
		if cobraCmd.Flags().Changed("cloud_ram") {
			other["cloud_os_ram"] = strconv.Itoa(cmdOsRAM)
		}
		if cobraCmd.Flags().Changed("cloud_flavor") {
			if cmdFlavor == "" {
				otherSet = true
			} else {
				other["cloud_flavor"] = cmdFlavor
			}
		}
		if cobraCmd.Flags().Changed("cloud_script") {
			if cmdPostCreationScript != "" {
				scriptContent, errp := internal.PathToContent(cmdPostCreationScript)
				if errp != nil {
					die(errp.Error())
				}
				other["cloud_script"] = scriptContent
			} else {
				other["cloud_script"] = ""
			}
		}
		if cobraCmd.Flags().Changed("cloud_config_files") {
			if cmdCloudConfigs == "" {
				otherSet = true
			} else {
				other["cloud_config_files"] = copyCloudConfigFiles(jq, cmdCloudConfigs)
			}
		}
		if cmdCloudSharedDisk {
			other["cloud_shared"] = "true"
		} else if cmdCloudSharedDiskUnset {
			other["cloud_shared"] = "false"
		}
		if len(other) > 0 || otherSet {
			req.Other = other
			req.OtherSet = true
			setReq = true
		}

		if setReq {
			jm.SetRequirements(req)
		}

		if cobraCmd.Flags().Changed("override") {
			jm.SetOverride(uint8(cmdOvr))
		}
		if cobraCmd.Flags().Changed("priority") {
			jm.SetPriority(uint8(cmdPri))
		}
		if cobraCmd.Flags().Changed("retries") {
			jm.SetRetries(uint8(cmdRet))
		}

		var deps jobqueue.Dependencies
		var depsSet bool
		if cobraCmd.Flags().Changed("cmd_deps") {
			cols := strings.Split(cmdCmdDeps, ",")
			if len(cols)%2 != 0 {
				die("--cmd_deps must have an even number of comma-separated entries")
			}
			deps = colsToDeps(cols)
			depsSet = true
		}
		if cobraCmd.Flags().Changed("deps") {
			deps = append(deps, groupsToDeps(cmdGroupDeps)...)
			depsSet = true
		}
		if depsSet {
			jm.SetDependencies(deps)
		}

		if cobraCmd.Flags().Changed("monitor_docker") {
			jm.SetMonitorDocker(cmdMonitorDocker)
		}

		var behaviours jobqueue.Behaviours
		var behavioursSet bool
		if cobraCmd.Flags().Changed("on_failure") {
			if cmdOnFailure == "" {
				cmdOnFailure = nothingBehaviour
			}
			var bjs jobqueue.BehavioursViaJSON
			err = json.Unmarshal([]byte(cmdOnFailure), &bjs)
			if err != nil {
				die("bad --on_failure: %s", err)
			}
			behaviours = bjs.Behaviours(jobqueue.OnFailure)
			behavioursSet = true
		}
		if cobraCmd.Flags().Changed("on_success") {
			if cmdOnSuccess == "" {
				cmdOnSuccess = nothingBehaviour
			}
			var bjs jobqueue.BehavioursViaJSON
			err = json.Unmarshal([]byte(cmdOnSuccess), &bjs)
			if err != nil {
				die("bad --on_success: %s", err)
			}
			behaviours = append(behaviours, bjs.Behaviours(jobqueue.OnSuccess)...)
			behavioursSet = true
		}
		if cobraCmd.Flags().Changed("on_exit") {
			if cmdOnExit == "" {
				cmdOnExit = nothingBehaviour
			}
			var bjs jobqueue.BehavioursViaJSON
			err = json.Unmarshal([]byte(cmdOnExit), &bjs)
			if err != nil {
				die("bad --on_exit: %s", err)
			}
			behaviours = append(behaviours, bjs.Behaviours(jobqueue.OnExit)...)
			behavioursSet = true
		}
		if behavioursSet {
			jm.SetBehaviours(behaviours)
		}

		if cobraCmd.Flags().Changed("mount_json") || cobraCmd.Flags().Changed("mounts") {
			if mountJSON == "" && mountSimple == "" {
				// unset mounts
				jm.SetMountConfigs(nil)
			} else {
				// set mounts
				jm.SetMountConfigs(mountParse(mountJSON, mountSimple))
			}
		}
		if cobraCmd.Flags().Changed("env") {
			errs := jm.SetEnvOverride(cmdEnv)
			if errs != nil {
				die("failed to handle env: %s", errs)
			}
		}

		// *** not really sure if it makes sense to be able to turn bsub mode
		// on and off; not allowing it for now
		// if cobraCmd.Flags().Changed("bsub") {
		// 	jm.SetBsubMode(deployment)
		// }

		// make the modifications
		jes := jobsToJobEssenses(jobs)
		modified, err := jq.Modify(jes, jm)
		if err != nil {
			die("failed to modify desired jobs: %s", err)
		}
		info("Modified %d incomplete, non-running commands (out of %d eligible)", len(modified), len(jobs))
		for to, from := range modified {
			fmt.Printf(" %s => %s\n", from, to)
		}
	},
}

func init() {
	RootCmd.AddCommand(modCmd)

	// flags specific to this sub-command
	modCmd.Flags().BoolVarP(&cmdAll, "all", "a", false, "modify all incomplete, non-running jobs")
	modCmd.Flags().StringVarP(&cmdIDStatus, "identifier", "i", "", "identifier of the commands you want to modify")
	modCmd.Flags().BoolVarP(&cmdIDIsSubStr, "search", "z", false, "treat -i as a substring to match against all report groups")
	modCmd.Flags().BoolVarP(&cmdIDIsInternal, "internal", "y", false, "treat -i as an internal job id")

	modCmd.Flags().StringVar(&cmdLine, "cmdline", "", "new command line")
	modCmd.Flags().StringVarP(&cmdLimitGroups, "limit_grps", "l", "", "comma-separated list of limit groups")
	// modCmd.Flags().StringVarP(&cmdDepGroups, "dep_grps", "e", "", "comma-separated list of dependency groups")
	modCmd.Flags().StringVarP(&cmdCwd, "cwd", "c", "", "base for the command's working dir")
	modCmd.Flags().BoolVar(&cmdCwdMatters, "cwd_matters", false, "--cwd should be used as the actual working directory")
	modCmd.Flags().BoolVar(&cmdCwdMattersUnset, "unset_cwd_matters", false, "unset --cwd_matters")
	modCmd.Flags().BoolVar(&cmdChangeHome, "change_home", false, "when not --cwd_matters, set $HOME to the actual working directory")
	modCmd.Flags().BoolVar(&cmdChangeHomeUnset, "unset_change_home", false, "unset --change_home")
	modCmd.Flags().StringVarP(&reqGroup, "req_grp", "g", "", "group name for commands with similar reqs")
	modCmd.Flags().StringVarP(&cmdMem, "memory", "m", "1G", "peak mem est. [specify units such as M for Megabytes or G for Gigabytes]")
	modCmd.Flags().StringVarP(&cmdTime, "time", "t", "1h", "max time est. [specify units such as m for minutes or h for hours]")
	modCmd.Flags().Float64Var(&cmdCPUs, "cpus", 1, "cpu cores needed")
	modCmd.Flags().IntVar(&cmdDisk, "disk", 0, "number of GB of disk space required (default 0)")
	modCmd.Flags().IntVarP(&cmdOvr, "override", "o", 0, "[0|1|2] should your mem/time estimates override? (default 0)")
	modCmd.Flags().IntVarP(&cmdPri, "priority", "p", 0, "[0-255] command priority (default 0)")
	modCmd.Flags().IntVarP(&cmdRet, "retries", "r", 3, "[0-255] number of automatic retries for failed commands")
	modCmd.Flags().StringVar(&cmdCmdDeps, "cmd_deps", "", "dependencies of your commands, in the form \"command1,cwd1,command2,cwd2...\"")
	modCmd.Flags().StringVarP(&cmdGroupDeps, "deps", "d", "", "dependencies of your commands, in the form \"dep_grp1,dep_grp2...\"")
	modCmd.Flags().StringVar(&cmdMonitorDocker, "monitor_docker", "", "monitor resource usage of docker container with given --name or --cidfile path")
	modCmd.Flags().StringVar(&cmdOnFailure, "on_failure", "", "behaviours to carry out when cmds fails, in JSON format")
	modCmd.Flags().StringVar(&cmdOnSuccess, "on_success", "", "behaviours to carry out when cmds succeed, in JSON format")
	modCmd.Flags().StringVar(&cmdOnExit, "on_exit", `[{"cleanup":true}]`, "behaviours to carry out when cmds finish running, in JSON format")
	modCmd.Flags().StringVarP(&mountJSON, "mount_json", "j", "", "remote file systems to mount, in JSON format; see 'wr mount -h'")
	modCmd.Flags().StringVar(&mountSimple, "mounts", "", "remote file systems to mount, as a ,-separated list of [c|u][r|w]:bucket[/path]; see 'wr mount -h'")
	modCmd.Flags().StringVar(&cmdOsPrefix, "cloud_os", "", "in the cloud, prefix name of the OS image servers that run the commands must use")
	modCmd.Flags().StringVar(&cmdOsUsername, "cloud_username", "", "in the cloud, username needed to log in to the OS image specified by --cloud_os")
	modCmd.Flags().IntVar(&cmdOsRAM, "cloud_ram", 0, "in the cloud, ram (MB) needed by the OS image specified by --cloud_os")
	modCmd.Flags().StringVar(&cmdFlavor, "cloud_flavor", "", "in the cloud, exact name of the server flavor that the commands must run on")
	modCmd.Flags().StringVar(&cmdPostCreationScript, "cloud_script", "", "in the cloud, path to a start-up script that will be run on the servers created to run these commands")
	modCmd.Flags().StringVar(&cmdCloudConfigs, "cloud_config_files", "", "in the cloud, comma separated paths of config files to copy to servers created to run these commands")
	modCmd.Flags().BoolVar(&cmdCloudSharedDisk, "cloud_shared", false, "mount /shared")
	modCmd.Flags().BoolVar(&cmdCloudSharedDiskUnset, "unset_cloud_shared", false, "unset --cloud_shared")
	modCmd.Flags().StringVar(&cmdEnv, "env", "", "comma-separated list of key=value environment variables to set before running the commands")
	// modCmd.Flags().BoolVar(&cmdBsubMode, "bsub", false, "enable bsub emulation mode")

	// *** user can't turn the bool options off, only on...

	modCmd.Flags().IntVar(&timeoutint, "timeout", 120, "how long (seconds) to wait to get a reply from 'wr manager'")
}
