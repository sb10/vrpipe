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

package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/spf13/cobra"
)

// maxScanTokenSize defines the size of bufio scan's buffer, enabling us to
// parse very long lines - longer than the max length of a command supported by
// shells such as bash.
const maxScanTokenSize = 4096 * 1024

// options for this cmd
var reqGroup string
var cmdTime string
var cmdMem string
var cmdCPUs float64
var cmdDisk int
var cmdOvr int
var cmdPri int
var cmdRet int
var cmdFile string
var cmdCwdMatters bool
var cmdChangeHome bool
var cmdRepGroup string
var cmdLimitGroups string
var cmdDepGroups string
var cmdCmdDeps string
var cmdGroupDeps string
var cmdOnFailure string
var cmdOnSuccess string
var cmdOnExit string
var cmdEnv string
var cmdReRun bool
var cmdOsPrefix string
var cmdOsUsername string
var cmdOsRAM int
var cmdBsubMode bool
var cmdPostCreationScript string
var cmdCloudConfigs string
var cmdCloudSharedDisk bool
var cmdFlavor string
var cmdQueue string
var cmdMisc string
var cmdMonitorDocker string
var rtimeoutint int
var simpleOutput bool

// addCmd represents the add command
var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add commands to the queue",
	Long: `Manually add commands you want run to the queue.

You can supply your commands by putting them in a text file (1 per line), or
by piping them in. In addition to the command itself, you can specify command-
specific options using a JSON object in (tab separated) column 2, or
alternatively have only a JSON object in column 1 that also specifies the
command as one of the name:value pairs. The possible options are:

cmd cwd cwd_matters change_home on_failure on_success on_exit mounts req_grp
memory time override cpus disk queue misc priority retries rep_grp dep_grps deps
cmd_deps monitor_docker cloud_os cloud_username cloud_ram cloud_script
cloud_config_files cloud_flavor cloud_shared env bsub_mode

If any of these will be the same for all your commands, you can instead specify
them as flags (which are treated as defaults in the case that they are
unspecified in the text file, but otherwise ignored). The meaning of each option
is detailed below.

A JSON object can written by starting and ending it with curly braces. Names and
single values are put in double quotes (except for numbers, which are left bare,
and booleans, where you write 'true' or 'false' without quotes) and the pair
separated with a colon, and pairs separated from each other with commas. Options
that take array values have their double-quoted values separated by commas and
enclosed in square brackets. For example (on one line): {"cmd":"myexe -f input >
output","cwd":"/path/to/cwd","priority":1,"dep_grps":["dg2","dg3"],"deps":
["dg1"]}

"cwd" determines the directory to cd to before running the command (the 'command
working directory'). If none is specified, the default will be your current
directory right now. (If adding to a remote cloud-deployed manager, then cwd
will instead default to /tmp.)

"cwd_matters" by default is false, causing "cwd" to taken as the parent
directory to create a unique working directory inside. This unique directory can
be deleted after the cmd finishes running (according to cleanup behaviour), and
enables tracking of how much disk space your cmd uses. If using mounts and not
specifying a mount point, the mount point will be the actual working directory.
It also sets $TMPDIR to a sister directory of the actual working directory, and
this is always deleted after the cmd runs. If, on the other hand, you set
cwd_matters, then "cwd" is the literal command working directory, you can't
clean up afterwards, you don't get disk space tracking and undefined mounts are
mounted in the "mnt" subdirectory of cwd. One benefit is that any output files
that your command creates with relative paths will be easy to find since they'll
be relative to your own set cwd path (otherwise you'd have to find out the
actual cwd value in the status of a job). It also lets you specify relative
paths to your input files in your cmd, assuming they are in your cwd.

"change_home" only has an effect when "cwd_matters" is false. If enabled, sets
the $HOME environment variable to the actual command working directory before
running the cmd.

"on_failure" determines what behaviours are triggered if your cmd exits non-0.
Behaviours are described using an array of objects, where each object has a key
corresponding to the name of the desired behaviour, and the relevant value. The
currently available behaviours are: "cleanup_all", which takes a boolean value
and if true will completely delete the actual working directory created when
cwd_matters is false (no effect when cwd_matters is true); "cleanup", which is
like cleanup_all except that it doesn't delete files that have been specified as
inputs or outputs [since you can't currently specify this, the current behaviour
is identical to cleanup_all]; "run", which takes a string command to run after
the main cmd runs; and "remove", which takes a boolean value and if true that
means that if the cmd gets buried, it will then immediately be removed from the
queue (useful for Cromwell compatibility).
For example [{"run":"cp error.log /shared/logs/this.log"},{"cleanup":true}]
would copy a log file that your cmd generated to describe its problems to some
shared location and then delete all files created by your cmd.

"on_success" is exactly like on_failure, except that the behaviours trigger when
your cmd exits 0.

"on_exit" is exactly like on_failure, except that the behaviours trigger when
your cmd exits, regardless of exit code. These behaviours will trigger after any
behaviours defined in on_failure or on_success.

"mounts" (or the --mount_json option) describes the remote file systems or
object stores you would like to be fuse mounted locally before running your
command. See the help text for 'wr mount' for an explanation of how to formulate
the value. Your mounts will be unmounted after the triggering of any behaviours,
so your "run" behaviours will be able to read from or write to anything in your
mount point(s). The "cleanup" and "cleanup_all" behaviours, however, will ignore
your mounted directories and any mount cache directories, so that nothing on
your remote file systems gets deleted. Unmounting will get rid of them though,
so you would still end up with a "cleaned" workspace.

"req_grp" is an arbitrary string that identifies the kind of commands you are
adding, such that future commands you add with this same requirements group are
likely to have similar memory and time requirements. It defaults to the basename
of the first word in your command, which it assumes to be the name of your
executable.

"memory" and "time" let you provide hints to wr manager so that it can do a
better job of spawning runners to handle these commands. "memory" values should
specify a unit, eg "100M" for 100 megabytes, or "1G" for 1 gigabyte. "time"
values should do the same, eg. "30m" for 30 minutes, or "1h" for 1 hour.

The manager learns how much memory and time commands in the same req_grp
actually used in the past, and will use its own values unless you set an
override. For this learning to work well, you should have reason to believe that
all the commands you add with the same req_grp will have similar memory and time
requirements, and you should pick the name in a consistent way such that you'll
use it again in the future.

For example, if you want to run an executable called "exop", and you know that
the memory and time requirements of exop vary with the size of its input file,
you might batch your commands so that all the input files in one batch have
sizes in a certain range, and then provide a req_grp that describes this, eg.
"exop.1-2Ginputs" for inputs in the 1 to 2 GB range.

(Don't name your req_grp after the expected requirements themselves, such as
"5GBram.1hr", because then the manager can't learn about your commands - it is
only learning about how good your estimates are! The name of your executable
should almost always be part of the req_grp name.)

"override" defines if your memory, disk or time should be used instead of the
manager's estimate. Possible values are:
0 = do not override wr's learned values for memory, disk and time (if any)
1 = override if yours are higher
2 = always override specified resource(s)
(If you choose to override eg. only disk, then the learned value for memory and
time will be used. If you want to override all 3 resources to disable learning
completly, you must explicitly supply non-zero values for memory and time and 0
or more for disk.)

"cpus" tells wr manager exactly how many CPU cores your command needs.

"disk" tells wr manager how much free disk space (in GB) your command needs.
Disk space reservation only applies to the OpenStack schedulers which will
create temporary volumes of the specified size if necessary. Note that disk
space usage checking and learning only occurs for jobs where cwd doesn't matter
(is a unique directory), and ignores the contents of mounted directories.

"queue" tells wr which queue a job should be submitted to, when using a job
scheduler that has queues (eg. LSF). If queue is not specified, wr will use
heuristics to pick the most appropriate queue based on the time, memory and cpu
requirements of the job.

"misc" will be used as-is to form the command line used to submit jobs to
external job schedulers (eg. LSF). For example, --misc '-R avx' might result
in a command line containing: bsub -R avx. To avoid quoting issues, surround
the --misc value in single quotes and if necessary use double quotes within the
value; do NOT use single quotes within the value. Eg. --misc '-R "foo bar"'.

"priority" defines how urgent a particular command is; those with higher
priorities will start running before those with lower priorities. The range of
possible values is 0 (default, for lowest priority) to 255 (highest priority).
Commands with the same priority will be started in the order they were added.
(Note, however, that order of starting is only guaranteed to hold true amongst
jobs with similar resource requirements, since your chosen job scheduler may,
for example, run your highest priority job on a machine where it takes up 90% of
memory, and then find another job to run on that machine that needs 10% or less
memory - and that job might be one of your low priority ones.)

"retries" defines how many times a command will be retried automatically if it
fails. Automatic retries are helpful in the case of transient errors, or errors
due to running out of memory or time (when retried, they will be retried with
more memory/time reserved). Once this number of retries is reached, the command
will be 'buried' until you take manual action to fix the problem and press the
retry button in the web interface.

"rep_grp" is an arbitrary group you can give your commands so you can query
their status later. This is only used for reporting and presentation purposes
when viewing status.

"limit_grps" is an array of arbitrary names you can associate with a command,
that can be used to limit the number of jobs that run at once in the same group.
You can optionally suffix a group name with :n where n is a integer new limit
for that group. 0 prevents jobs in that group running at all. -1 makes jobs in
that group unlimited. If no limit number is suffixed, groups will be unlimited
until a limit is set with the "wr limit" command.

"dep_grps" is an array of arbitrary names you can associate with a command, so
that you can then refer to this job (and others with the same dep_grp) in
another job's deps.

"deps" or "cmd_deps" define the dependencies of this command. The commands that
these refer to must complete before this command will start. The value for
"deps" is an array of the dep_grp of other commands. Dependencies specified in
this way are 'live', causing this command to be automatically re-run if any
commands with any of the dep_grps it is dependent upon get added to the queue.
The value for "cmd_deps" is an array of JSON objects with "cmd" and "cwd"
name:value pairs (if cwd doesn't matter for a cmd, provide it as an empty
string). These are static dependencies; once resolved they do not get re-
evaluated.

"monitor_docker" turns on monitoring of a docker container identified by the
given string, which could be the container's --name or path to its --cidfile. If
the string contains ? or * symbols and doesn't match a name or file name
literally, those symbols will be treated as wildcards (any single character, or
any number of any character, respectively) in a search for the first matching
file name containing a valid container id, to be treated as the --cidfile.
This will add the container's peak RAM and total CPU usage to the reported RAM
and CPU usage of this job. If the special argument "?" is supplied, monitoring
will apply to the first new docker container that appears after the command
starts to run. NB: in ? mode, if multiple jobs that run docker containers start
running at the same time on the same machine, the reported stats could be wrong
for one or more of those jobs. Requires that docker is installed on the machine
where the job will run (and that the command uses docker to run a container).
NB: does not handle monitoring of multiple docker containers run by a single
command. A side effect of monitoring a container is that if you use wr to kill
the job for this command, wr will also kill the container.

The "cloud_*" related options let you override the defaults of your cloud
deployment. For example, if you do 'wr cloud deploy --os "Ubuntu 16" --os_ram
2048 -u ubuntu -s ~/my_ubuntu_post_creation_script.sh', any commands you add
will by default run on cloud nodes running Ubuntu. If you set "cloud_os" to
"CentOS 7", "cloud_username" to "centos", "cloud_ram" to 4096, and
"cloud_script" to "~/my_centos_post_creation_script.sh", then this command will
run on a cloud node running CentOS (with at least 4GB ram). If you set
"cloud_flavor" then the command will only run on a server with that exact
flavor (normally the cheapest flavor is chosen for you based on the command's
resource requirements). The format for cloud_config_files is described under the
help text for "wr cloud deploy"'s --config_files option. The per-job config
files you specify will be treated as in addition to any specified during cloud
deploy or when starting the manager. Note that your cloud_script must complete
within 15 mins; if your script is slow because it installs a lot of software,
consider creating a new image instead and using cloud_os.

"cloud_shared" only works when using a cloud scheduler where both the manager
and jobs will run on Ubuntu. It will cause /shared on the manager's server to be
NFS shared to /shared mounted on the server where your job runs. This gives you
an easy way of having a shared disk in the cloud, but the size of that disk is
limited to the size of the manager's volume. Performance may also be poor. This
is only intended when you need a little bit of shared state between jobs, not
for writing lots of large files. (If you need a high performance shared disk,
don't use this option, and instead set up your own shared filesystem, eg.
GlusterFS, and specify a cloud_script that mounts it.)

"env" is an array of "key=value" environment variables, which override or add to
the environment variables the command will see when it runs. The base variables
that are overwritten depend on if you run 'wr add' on the same machine as you
started the manager (local, vs remote). In the local case, commands will use
base variables as they were at the moment in time you run 'wr add', so to set a
certain environment variable for all commands, you could instead just set it
prior to calling 'wr add'. In the remote case the command will use base
variables as they were on the machine where the command is executed when that
machine was started.

"bsub_mode" is a boolean that results in the job being assigned a unique (for
this manager session) job id, and turns on bsub emulation, which means that if
your Cmd calls bsub, it will instead result in a command being added to wr. The
new job will have this job's mount and cloud_* options.`,
	Run: func(combraCmd *cobra.Command, args []string) {
		ctx := context.Background()
		// check the command line options
		if cmdFile == "" {
			die("--file is required")
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

		jobs, isLocal, defaultedRepG := parseCmdFile(ctx, jq, combraCmd.Flags().Changed("disk"))

		var envVars []string
		if isLocal {
			envVars = os.Environ()
		}

		// add the jobs to the queue *** should add at most 1,000,000 jobs at a
		// time to avoid time out issues...
		if simpleOutput {
			ids, err := jq.AddAndReturnIDs(jobs, envVars, !cmdReRun)
			if err != nil {
				die("%s", err)
			}
			if len(ids) == 0 {
				os.Exit(1)
			}
			for _, id := range ids {
				fmt.Printf("%s\n", id)
			}
		} else {
			inserts, dups, err := jq.Add(jobs, envVars, !cmdReRun)
			if err != nil {
				die("%s", err)
			}

			if defaultedRepG {
				info("Added %d new commands (%d were duplicates) to the queue using default identifier '%s'", inserts, dups, cmdRepGroup)
			} else {
				info("Added %d new commands (%d were duplicates) to the queue", inserts, dups)
			}
		}
	},
}

func init() {
	RootCmd.AddCommand(addCmd)

	// flags specific to this sub-command
	addCmd.Flags().StringVarP(&cmdFile, "file", "f", "-", "file containing your commands; - means read from STDIN")
	addCmd.Flags().StringVarP(&cmdRepGroup, "rep_grp", "i", "manually_added", "reporting group for your commands")
	addCmd.Flags().StringVarP(&cmdLimitGroups, "limit_grps", "l", "", "comma-separated list of limit groups")
	addCmd.Flags().StringVarP(&cmdDepGroups, "dep_grps", "e", "", "comma-separated list of dependency groups")
	addCmd.Flags().StringVarP(&cmdCwd, "cwd", "c", "", "base for the command's working dir")
	addCmd.Flags().BoolVar(&cmdCwdMatters, "cwd_matters", false, "--cwd should be used as the actual working directory")
	addCmd.Flags().BoolVar(&cmdChangeHome, "change_home", false, "when not --cwd_matters, set $HOME to the actual working directory")
	addCmd.Flags().StringVarP(&reqGroup, "req_grp", "g", "", "group name for commands with similar reqs")
	addCmd.Flags().StringVarP(&cmdMem, "memory", "m", "1G", "peak mem est. [specify units such as M for Megabytes or G for Gigabytes]")
	addCmd.Flags().StringVarP(&cmdTime, "time", "t", "1h", "max time est. [specify units such as m for minutes or h for hours]")
	addCmd.Flags().Float64Var(&cmdCPUs, "cpus", 1, "cpu cores needed")
	addCmd.Flags().IntVar(&cmdDisk, "disk", 0, "number of GB of disk space required (default 0)")
	addCmd.Flags().IntVarP(&cmdOvr, "override", "o", 0, "[0|1|2] should your mem/time estimates override? (default 0)")
	addCmd.Flags().IntVarP(&cmdPri, "priority", "p", 0, "[0-255] command priority (default 0)")
	addCmd.Flags().IntVarP(&cmdRet, "retries", "r", 3, "[0-255] number of automatic retries for failed commands")
	addCmd.Flags().StringVar(&cmdCmdDeps, "cmd_deps", "", "dependencies of your commands, in the form \"command1,cwd1,command2,cwd2...\"")
	addCmd.Flags().StringVarP(&cmdGroupDeps, "deps", "d", "", "dependencies of your commands, in the form \"dep_grp1,dep_grp2...\"")
	addCmd.Flags().StringVar(&cmdMonitorDocker, "monitor_docker", "", "monitor resource usage of docker container with given --name or --cidfile path")
	addCmd.Flags().StringVar(&cmdOnFailure, "on_failure", "", "behaviours to carry out when cmds fails, in JSON format")
	addCmd.Flags().StringVar(&cmdOnSuccess, "on_success", "", "behaviours to carry out when cmds succeed, in JSON format")
	addCmd.Flags().StringVar(&cmdOnExit, "on_exit", `[{"cleanup":true}]`, "behaviours to carry out when cmds finish running, in JSON format")
	addCmd.Flags().StringVarP(&mountJSON, "mount_json", "j", "", "remote file systems to mount, in JSON format; see 'wr mount -h'")
	addCmd.Flags().StringVar(&mountSimple, "mounts", "", "remote file systems to mount, as a ,-separated list of [c|u][r|w]:bucket[/path]; see 'wr mount -h'")
	addCmd.Flags().StringVar(&cmdOsPrefix, "cloud_os", "", "in the cloud, prefix name of the OS image servers that run the commands must use")
	addCmd.Flags().StringVar(&cmdOsUsername, "cloud_username", "", "in the cloud, username needed to log in to the OS image specified by --cloud_os")
	addCmd.Flags().IntVar(&cmdOsRAM, "cloud_ram", 0, "in the cloud, ram (MB) needed by the OS image specified by --cloud_os")
	addCmd.Flags().StringVar(&cmdFlavor, "cloud_flavor", "", "in the cloud, exact name of the server flavor that the commands must run on")
	addCmd.Flags().StringVar(&cmdPostCreationScript, "cloud_script", "", "in the cloud, path to a start-up script that will be run on the servers created to run these commands")
	addCmd.Flags().StringVar(&cmdCloudConfigs, "cloud_config_files", "", "in the cloud, comma separated paths of config files to copy to servers created to run these commands")
	addCmd.Flags().BoolVar(&cmdCloudSharedDisk, "cloud_shared", false, "mount /shared")
	addCmd.Flags().StringVar(&cmdQueue, "queue", "", "name of queue to submit to, for schedulers with queues")
	addCmd.Flags().StringVar(&cmdMisc, "misc", "", "miscellaneous options to pass through to scheduler when submitting")
	addCmd.Flags().StringVar(&cmdEnv, "env", "", "comma-separated list of key=value environment variables to set before running the commands")
	addCmd.Flags().BoolVar(&cmdReRun, "rerun", false, "re-run any commands that you add that had been previously added and have since completed")
	addCmd.Flags().BoolVar(&cmdBsubMode, "bsub", false, "enable bsub emulation mode")

	addCmd.Flags().IntVar(&timeoutint, "timeout", 120, "how long (seconds) to wait to get a reply from 'wr manager'")
	addCmd.Flags().IntVar(&rtimeoutint, "reserve_timeout", 1, "how long (seconds) to wait before a runner exits when there is no more work'")
	addCmd.Flags().BoolVarP(&simpleOutput, "simple", "s", false, "simplify output to only queued job ids")

	err := addCmd.Flags().MarkHidden("reserve_timeout")
	if err != nil {
		die("cloud not hide reserver_timeout option: %s", err)
	}
}

// convert cmd,cwd columns in to Dependency.
func colsToDeps(cols []string) (deps jobqueue.Dependencies) {
	for i := 0; i < len(cols); i += 2 {
		deps = append(deps, jobqueue.NewEssenceDependency(cols[i], cols[i+1]))
	}
	return
}

// convert group1,group2,... in to a Dependency.
func groupsToDeps(groups string) (deps jobqueue.Dependencies) {
	for _, depgroup := range strings.Split(groups, ",") {
		deps = append(deps, jobqueue.NewDepGroupDependency(depgroup))
	}
	return
}

// parseCmdFile reads the given cmd file to get desired jobs, modified by
// defaults specified in other command line args. Returns job slice, bool for if
// the manager is on the same host as us, and bool for if any job defaulted to
// the default repgrp.
func parseCmdFile(ctx context.Context, jq *jobqueue.Client, diskSet bool) ([]*jobqueue.Job, bool, bool) {
	var isLocal bool
	currentIP, errc := internal.CurrentIP("")
	if errc != nil {
		warn("Could not get current IP: %s", errc)
	}
	if currentIP+":"+config.ManagerPort == jq.ServerInfo.Addr {
		isLocal = true
	}

	// if the manager is remote, copy over any cloud config files to unique
	// locations, and adjust cloudConfigFiles to make sense from the manager's
	// perspective
	if !isLocal && cmdCloudConfigs != "" {
		cmdCloudConfigs = copyCloudConfigFiles(jq, cmdCloudConfigs)
	}

	bsubMode := ""
	if cmdBsubMode {
		bsubMode = deployment
	}

	if cmdCPUs < 0 {
		die("--cpus can't be negative")
	}

	jd := &jobqueue.JobDefaults{
		RepGrp:           cmdRepGroup,
		ReqGrp:           reqGroup,
		Cwd:              cmdCwd,
		CwdMatters:       cmdCwdMatters,
		ChangeHome:       cmdChangeHome,
		CPUs:             cmdCPUs,
		Disk:             cmdDisk,
		DiskSet:          diskSet,
		Override:         cmdOvr,
		Priority:         cmdPri,
		Retries:          cmdRet,
		Env:              cmdEnv,
		MonitorDocker:    cmdMonitorDocker,
		CloudOS:          cmdOsPrefix,
		CloudUser:        cmdOsUsername,
		CloudScript:      cmdPostCreationScript,
		CloudConfigFiles: cmdCloudConfigs,
		CloudOSRam:       cmdOsRAM,
		CloudFlavor:      cmdFlavor,
		CloudShared:      cmdCloudSharedDisk,
		SchedulerQueue:   cmdQueue,
		SchedulerMisc:    cmdMisc,
		BsubMode:         bsubMode,
		RTimeout:         rtimeoutint,
	}

	if jd.RepGrp == "" {
		jd.RepGrp = "manually_added"
	}

	var err error
	if cmdMem == "" {
		jd.Memory = 0
	} else {
		mb, errf := bytefmt.ToMegabytes(cmdMem)
		if errf != nil {
			die("--memory was not specified correctly: %s", errf)
		}
		jd.Memory = int(mb)
	}
	if cmdTime == "" {
		jd.Time = 0 * time.Second
	} else {
		jd.Time, err = time.ParseDuration(cmdTime)
		if err != nil {
			die("--time was not specified correctly: %s", err)
		}
	}

	if cmdLimitGroups != "" {
		jd.LimitGroups = strings.Split(cmdLimitGroups, ",")
	}

	if cmdDepGroups != "" {
		jd.DepGroups = strings.Split(cmdDepGroups, ",")
	}

	if cmdCmdDeps != "" {
		cols := strings.Split(cmdCmdDeps, ",")
		if len(cols)%2 != 0 {
			die("--cmd_deps must have an even number of comma-separated entries")
		}
		jd.Deps = colsToDeps(cols)
	}
	if cmdGroupDeps != "" {
		jd.Deps = append(jd.Deps, groupsToDeps(cmdGroupDeps)...)
	}

	if cmdOnFailure != "" {
		var bjs jobqueue.BehavioursViaJSON
		err = json.Unmarshal([]byte(cmdOnFailure), &bjs)
		if err != nil {
			die("bad --on_failure: %s", err)
		}
		jd.OnFailure = bjs.Behaviours(jobqueue.OnFailure)
	}
	if cmdOnSuccess != "" {
		var bjs jobqueue.BehavioursViaJSON
		err = json.Unmarshal([]byte(cmdOnSuccess), &bjs)
		if err != nil {
			die("bad --on_success: %s", err)
		}
		jd.OnSuccess = bjs.Behaviours(jobqueue.OnSuccess)
	}
	if cmdOnExit != "" {
		var bjs jobqueue.BehavioursViaJSON
		err = json.Unmarshal([]byte(cmdOnExit), &bjs)
		if err != nil {
			die("bad --on_exit: %s", err)
		}
		jd.OnExit = bjs.Behaviours(jobqueue.OnExit)
	}

	if mountJSON != "" || mountSimple != "" {
		jd.MountConfigs = mountParse(mountJSON, mountSimple)
	}

	// open file or set up to read from STDIN
	var reader io.Reader
	if cmdFile == "-" {
		reader = os.Stdin
	} else {
		reader, err = os.Open(cmdFile)
		if err != nil {
			die("could not open file '%s': %s", cmdFile, err)
		}
		defer internal.LogClose(ctx, reader.(*os.File), "cmds file", "path", cmdFile)
	}

	// we'll default to pwd if the manager is on the same host as us, or if
	// cwd matters, /tmp otherwise (and cmdCwd has not been supplied)
	var pwd string
	var remoteWarning bool
	if cmdCwd == "" {
		wd, errg := os.Getwd()
		if errg != nil {
			die("%s", errg)
		}
		if isLocal || cmdCwdMatters {
			pwd = wd
		} else {
			pwd = "/tmp"
			remoteWarning = true
		}
	}

	// for network efficiency, read in all commands and create a big slice
	// of Jobs and Add() them in one go afterwards
	var jobs []*jobqueue.Job
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)
	defaultedRepG := false
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		cols := strings.Split(scanner.Text(), "\t")
		colsn := len(cols)
		if colsn < 1 || cols[0] == "" {
			continue
		}
		if colsn > 2 {
			die("line %d has too many columns; check `wr add -h`", lineNum)
		}

		// determine all the options for this command
		var jvj *jobqueue.JobViaJSON
		var jsonErr error
		if colsn == 2 {
			jsonErr = json.Unmarshal([]byte(cols[1]), &jvj)
			if jsonErr == nil {
				jvj.Cmd = cols[0]
			}
		} else {
			if strings.HasPrefix(cols[0], "{") {
				jsonErr = json.Unmarshal([]byte(cols[0]), &jvj)
			} else {
				jvj = &jobqueue.JobViaJSON{Cmd: cols[0]}
			}
		}

		if jsonErr != nil {
			die("line %d had a problem with the JSON: %s", lineNum, jsonErr)
		}

		if jvj.CPUs != nil && *jvj.CPUs < 0 {
			die("line %d has a negative cpus count", lineNum)
		}

		if jvj.Cwd == "" && jd.Cwd == "" {
			if remoteWarning {
				warn("command working directories defaulting to %s since the manager is running remotely", pwd)
			}
			jd.Cwd = pwd
		}

		if jvj.RepGrp == "" {
			defaultedRepG = true
		}

		if !isLocal && jvj.CloudConfigFiles != "" {
			jvj.CloudConfigFiles = copyCloudConfigFiles(jq, jvj.CloudConfigFiles)
		}

		job, errf := jvj.Convert(jd)
		if errf != nil {
			die("line %d had a problem: %s", lineNum, errf)
		}

		jobs = append(jobs, job)
	}

	serr := scanner.Err()
	if serr != nil {
		die("failed to read whole file: %s", serr.Error())
	}

	return jobs, isLocal, defaultedRepG
}

// copyCloudConfigFiles copies local config files to the manager's machine to a
// path based on the file's MD5, and then returns an altered input value to use
// the MD5 paths as the sources, keeping the desired destinations. It does not
// alter path specs for config files that don't exist locally.
func copyCloudConfigFiles(jq *jobqueue.Client, configFiles string) string {
	cfs := strings.Split(configFiles, ",")
	remoteConfigFiles := make([]string, 0, len(cfs))
	for _, cf := range cfs {
		parts := strings.Split(cf, ":")
		local := internal.TildaToHome(parts[0])
		_, err := os.Stat(local)
		if err != nil {
			remoteConfigFiles = append(remoteConfigFiles, cf)
			continue
		}

		var desired string
		if len(parts) == 2 {
			desired = parts[1]
		} else {
			desired = parts[0]
		}

		remote, err := jq.UploadFile(local, "")
		if err != nil {
			warn("failed to upload [%s] to a unique location: %s", local, err)
			remoteConfigFiles = append(remoteConfigFiles, cf)
			continue
		}

		remoteConfigFiles = append(remoteConfigFiles, remote+":"+desired)
	}
	return strings.Join(remoteConfigFiles, ",")
}
