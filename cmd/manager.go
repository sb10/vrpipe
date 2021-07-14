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

package cmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	sync "github.com/sasha-s/go-deadlock"
	"github.com/wtsi-ssg/wr/clog"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/VertebrateResequencing/wr/kubernetes/client"
	"github.com/kardianos/osext"
	"github.com/sb10/waitgroup"
	"github.com/sevlyar/go-daemon"
	"github.com/spf13/cobra"
)

// options for this cmd
var foreground bool
var scheduler string
var localUsername string
var backupPath string
var managerTimeoutSeconds int
var managerDebug bool
var maxServers int
var maxLocalCores int
var maxLocalRAM int
var cloudNoSecurityGroups bool
var cloudUseConfigDrive bool
var useCertDomain bool
var runnerDebug bool

const kubernetes = "kubernetes"
const deadlockTimeout = 5 * time.Minute

// managerCmd represents the manager command
var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Workflow manager",
	Long: `The workflow management system.

The wr manager works in the background, doing all the work of ensuring your
commands get run successfully.

It maintains both a temporary queue of the commands you want to run, and a
permanent history of commands you've run in the past. As commands are added to
the queue, it makes sure to spawn sufficient 'wr runner' agents to get them all
run.

You'll need to start this daemon with the 'start' sub-command before you can
achieve anything useful with the other wr commands.

If the background process that is spawned when you run this dies, any commands
from your workflows that were running at the time will continue to run. If
they're still running when you start the manager again, the new manager will
pick them up and things will continue normally. If they finish running while the
manager is dead, you have 24hrs to start the manager again; if you do so then
their completed state will be recorded and things will continue normally. If
more than 24hrs pass, however, the fact that the commands completed will not be
known by the new manager, and they will eventually appear in "lost contact"
state. You will have to then confirm them as dead and retry them from the start
(even though they had actually completed).`,
}

// start sub-command starts the daemon
var managerStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start workflow management",
	Long: `Start the workflow manager, daemonizing it in to the background
(unless --foreground option is supplied).

If the manager fails to start or dies unexpectedly, you can check the logs which
are by default found in ~/.wr_[deployment]/log.

To use the openstack scheduler, see 'wr cloud deploy -h' for the details of
which environment variables you need to use. That help also explains some of the
--cloud* options in further detail.

If using the openstack scheduler, note that you must be running on an OpenStack
server already. Be sure to set --local_username to your username outside of the
cloud, so that resources created will not conflict with anyone else in your
tenant (project) also running wr. When wr creates worker instances, they will
automatically use the same networks as the manager's server. The first network
attached to workers will be the one that matches the --cloud_cidr, and the
default (if any) and a custom security group will be applied. If there are any
other networks, they will also be attached to the worker instances, but with no
security groups.

Instead of creating your own openstack network and instance to start the
manager on, you can use 'wr cloud deploy -p openstack' to create an OpenStack
server on which wr manager will be started in OpenStack mode for you. 

Similarly, If using the Kubernetes scheduler you must already be running in a
pod. Be sure to pass a namespace for wr to use that will not have another wr
user attempting to use it.
Instead it is recommended to use 'wr k8s deploy' to bootstrap wr to a cluster.

The --use_cert_domain option is intended for use when you have configured your
own security certificates and want the manager to be reachable at a given
domain name, because there is a risk of the manager's server going down and you
want to be able to bring a new server up (at potentially a different IP address)
and have clients on other servers be able to reconnect to the new manager (after
you set the domain to point to the new manager's server's IP).

If you want to start multiple managers up in different OpenStack networks that
you've created yourself, note that --local_username will need to be globally
unique, since it is used to name the private key that will be created in
OpenStack, and if a key with that name already exists, the manager will not be
able to create a new one (or get the existing one), and so will not function
fully.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		// first we need our working directory to exist
		createWorkingDir()

		// check to see if the manager is already running (regardless of the
		// state of the pid file), giving us a meaningful error message in the
		// most obvious case of failure to start
		jq := connect(1*time.Second, true)
		if jq != nil {
			die("wr manager on port %s is already running (pid %d)", config.ManagerPort, jq.ServerInfo.PID)
		}

		var postCreation []byte
		var extraArgs []string
		if postCreationScript != "" {
			var err error
			postCreation, err = os.ReadFile(postCreationScript)
			if err != nil {
				die("--cloud_script %s could not be read: %s", postCreationScript, err)
			}

			// daemon runs from /, so we need to convert relative to absolute
			// path *** and then pretty hackily, re-specify the option by
			// repeating it on the end of os.Args, where the daemonization code
			// will pick it up
			pcsAbs, err := filepath.Abs(postCreationScript)
			if err != nil {
				die("--cloud_script %s could not be converted to an absolute path: %s", postCreationScript, err)
			}
			if pcsAbs != postCreationScript {
				extraArgs = append(extraArgs, "--cloud_script")
				extraArgs = append(extraArgs, pcsAbs)
			}
		}

		if scheduler == kubernetes {
			if len(kubeNamespace) == 0 {
				die("namespace must be specified when using the kubernetes scheduler")
			}
			if len(configMapName) == 0 && len(postCreationScript) == 0 {
				die("either a config map name or path to a post creation script is required")
			}
		}

		if len(localUsername) > maxCloudResourceUsernameLength {
			die("--local_username must be %d characters or less", maxCloudResourceUsernameLength)
		}

		// later, we will wait for the daemonized manager to either create a new
		// token file, or if we already have one, to touch it, so we store the
		// time now to know when the touch happens
		preStart := time.Now()

		// if we already have a token file, warn the user
		if _, errs := os.Stat(config.ManagerTokenFile); errs == nil {
			warn("will re-use the existing token in %s", config.ManagerTokenFile)
		}

		// now daemonize unless in foreground mode
		if foreground {
			syscall.Umask(config.ManagerUmask)
			startJQ(ctx, postCreation)
		} else {
			child, context := daemonize(config.ManagerPidFile, config.ManagerUmask, extraArgs...)
			if child != nil {
				// parent; wait a while for our child to bring up the manager
				// before exiting
				mTimeout := time.Duration(managerTimeoutSeconds) * time.Second
				internal.WaitForFile(config.ManagerTokenFile, preStart, mTimeout)
				jq := connect(mTimeout, true)
				if jq == nil {
					// display any error or crit lines in the log
					f, errf := os.Open(config.ManagerLogFile)
					if errf == nil {
						scanner := bufio.NewScanner(f)
						for scanner.Scan() {
							line := scanner.Text()
							if strings.Contains(line, "lvl=crit") || strings.Contains(line, "lvl=eror") {
								fmt.Println(line)
							}
						}
					}
					die("wr manager failed to start on port %s after %ds", config.ManagerPort, managerTimeoutSeconds)
				}
				token, err := token()
				if err != nil {
					warn("token could not be read! [%s]", err)
				}
				logStarted(jq.ServerInfo, token)
			} else {
				// daemonized child, that will run until signalled to stop
				defer func() {
					err := context.Release()
					if err != nil {
						warn("daemon release failed: %s", err)
					}
				}()
				startJQ(ctx, postCreation)
			}
		}
	},
}

// stop sub-command stops the daemon by sending it a term signal
var managerStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop workflow management",
	Long: `Immediately stop the workflow manager, saving its state.

Note that any runners that are currently running will die, along with any
commands they were running. It is more graceful to use 'drain' instead.`,
	Run: func(cmd *cobra.Command, args []string) {
		// the daemon could be running but be non-responsive, or it could have
		// exited but left the pid file in place; to best cover all
		// eventualities we check the pid file first, try and terminate its pid,
		// then confirm we can't connect
		pid, err := daemon.ReadPidFile(config.ManagerPidFile)
		var stopped bool
		if err == nil {
			stopped = stopdaemon(pid, "pid file "+config.ManagerPidFile)
		} else {
			// probably no pid file, we'll see if the daemon is up by trying to
			// connect
			jq := connect(1*time.Second, true)
			if jq == nil {
				die("wr manager does not seem to be running on port %s", config.ManagerPort)
			}
		}

		var jq *jobqueue.Client
		if stopped {
			// we'll do a quick test to confirm the daemon is down
			jq = connect(1*time.Second, true)
			if jq != nil {
				warn("according to the pid file %s, wr manager was running with pid %d, and I terminated that pid, but the manager is still up on port %s!", config.ManagerPidFile, pid, config.ManagerPort)
			} else {
				info("wr manager running on port %s was gracefully shut down", config.ManagerPort)
				deleteToken()
				return
			}
		} else {
			// we failed to SIGTERM the pid in the pid file, let's take some
			// time to confirm the daemon is really up
			jq = connect(5*time.Second, true)
			if jq == nil {
				die("according to the pid file %s, wr manager for port %s was running with pid %d, but that process could not be terminated and the manager could not be connected to; most likely the pid file is wrong and the manager is not running - after confirming, delete the pid file before trying to start the manager again", config.ManagerPidFile, config.ManagerPort, pid)
			}
		}

		// we managed to connect to the daemon; try to stop it again using its
		// real pid; though it may actually be running on a remote host and we
		// managed to connect to it via ssh port forwarding; compare the server
		// ip to our own
		currentIP, err := internal.CurrentIP("")
		if err != nil {
			warn("Could not get current IP: %s", err)
		}
		myAddr := currentIP + ":" + config.ManagerPort
		sAddr := jq.ServerInfo.Addr
		if myAddr == sAddr {
			err = jq.Disconnect()
			if err != nil {
				warn("Disconnecting from the server failed: %s", err)
			}
			stopped = stopdaemon(jq.ServerInfo.PID, "the manager itself")
		} else {
			// use the client command to stop it
			stopped = jq.ShutdownServer()

			// since I don't trust using a client connection to shut down the
			// server, double check I can no longer connect
			if stopped {
				jq = connect(1*time.Second, true)
				if jq != nil {
					warn("I requested shut down of the remote manager at %s, but it's still up!", sAddr)
					stopped = false
				}
			}
		}

		if stopped {
			info("wr manager running at %s was gracefully shut down", sAddr)
			deleteToken()
		} else {
			die("I've tried everything; giving up trying to stop the manager at %s", sAddr)
		}
	},
}

// drain sub-command makes the server stop spawning new runners and stops it
// letting existing runners reserve jobs, and when there are no more runners
// running it will exit by itself
var managerDrainCmd = &cobra.Command{
	Use:   "drain",
	Short: "Drain the workflow manager of running jobs and then stop",
	Long: `Wait for currently running jobs to finish and then gracefully stop the workflow manager, saving its state.

While draining you can continue to add new Jobs, but nothing new will start
running until the drain completes (or the manager is stopped) and the manager is
then started again.

It is safe to repeat this command to get an update on how long before the drain
completes.

NB: if using 'wr cloud deploy --deployment production', do not use drain without
also configuring an S3 location for your database backup, as otherwise any
changes to the database between calling drain and the manager finally shutting
down will be lost.`,
	Run: func(cmd *cobra.Command, args []string) {
		// first try and connect
		jq := connect(5*time.Second, true)
		if jq == nil {
			die("could not connect to the manager on port %s, so could not initiate a drain; has it already been stopped?", config.ManagerPort)
			// *** this would happen after calling drain a few times and the
			// manager has finally stopped itself, but we don't know if the
			// the manager stopped cleanly in response to our drain, or if it
			// crashed and there are still runners, so we can't deleteToken()...
		}

		// we managed to connect to the daemon; ask it to go in to drain mode
		numLeft, etc, err := jq.DrainServer()
		if err != nil {
			die("even though I was able to connect to the manager, it failed to enter drain mode: %s", err)
		}

		completeMsg := "have already completed"
		if etc > 0 {
			completeMsg = fmt.Sprintf("complete in less than %s", etc)
		}

		if numLeft == 0 {
			info("wr manager running on port %s is drained: there were no jobs still running, so the manger should stop right away.", config.ManagerPort)
			deleteToken()
		} else if numLeft == 1 {
			info("wr manager running on port %s is now draining; there is a job still running, and it should %s",
				config.ManagerPort, completeMsg)
		} else {
			info("wr manager running on port %s is now draining; there are %d jobs still running, and they should %s",
				config.ManagerPort, numLeft, completeMsg)
		}

		err = jq.Disconnect()
		if err != nil {
			warn("disconnecting from the server failed: %s", err)
		}
	},
}

// pause sub-command makes the server stop spawning new runners and stops it
// letting existing runners reserve jobs. It's like drain, but you can resume.
var managerPauseCmd = &cobra.Command{
	Use:   "pause",
	Short: "Pause starting any new jobs",
	Long: `Pause starting any new jobs; allowing running jobs to continue.

While paused you can continue to add new Jobs, but nothing new will start
running until you "resume". This is like the "drain" command, but doesn't stop
the manager.

It is safe to repeat this command to get an update on how long before your last
running job will finish.`,
	Run: func(cmd *cobra.Command, args []string) {
		// first try and connect
		jq := connect(5*time.Second, true)
		if jq == nil {
			die("could not connect to the manager on port %s, so could not initiate a pause", config.ManagerPort)
		}

		// we managed to connect to the daemon; ask it to go in to pause mode
		numLeft, etc, err := jq.PauseServer()
		if err != nil {
			die("even though I was able to connect to the manager, it failed to enter pause mode: %s", err)
		}

		if numLeft == 0 {
			info("wr manager running on port %s is paused: there were no jobs still running.", config.ManagerPort)
		} else if numLeft == 1 {
			info("wr manager running on port %s is now paused; there is a job still running, and it should complete in less than %s", config.ManagerPort, etc)
		} else {
			info("wr manager running on port %s is now paused; there are %d jobs still running, and they should complete in less than %s", config.ManagerPort, numLeft, etc)
		}

		err = jq.Disconnect()
		if err != nil {
			warn("disconnecting from the server failed: %s", err)
		}
	},
}

// resume sub-command makes the server start spawning new runners. For use after
// a pause.
var managerResumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume starts running queued jobs following a pause",
	Long: `Resume starts running queued jobs following a pause.

If you have used the "pause" command, running this will resume normal operation
of the manager.`,
	Run: func(cmd *cobra.Command, args []string) {
		// first try and connect
		jq := connect(5*time.Second, true)
		if jq == nil {
			die("could not connect to the manager on port %s, so could not initiate a pause", config.ManagerPort)
		}

		// we managed to connect to the daemon; ask it to resume
		err := jq.ResumeServer()
		if err != nil {
			die("even though I was able to connect to the manager, it failed to resume: %s", err)
		}

		info("wr manager running on port %s has resumed", config.ManagerPort)

		err = jq.Disconnect()
		if err != nil {
			warn("disconnecting from the server failed: %s", err)
		}
	},
}

// status sub-command tells if the manger is up or down
var managerStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get status of the workflow manager",
	Long:  `Find out if the workflow manager is currently running or not.`,
	Run: func(cmd *cobra.Command, args []string) {
		// see if pid file suggests it is supposed to be running
		pid, err := daemon.ReadPidFile(config.ManagerPidFile)
		if err == nil {
			// confirm
			jq := connect(5 * time.Second)
			if jq != nil {
				reportLiveStatus(jq)
				return
			}

			die("wr manager on port %s is supposed to be running with pid %d, but is non-responsive",
				config.ManagerPort, pid)
		}

		// no pid file, so it's supposed to be down; confirm
		jq := connect(1*time.Second, true)
		if jq == nil {
			fmt.Println("stopped")
		} else {
			reportLiveStatus(jq)
		}
	},
}

// backup sub-command does a database backup
var managerBackupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup wr's database",
	Long: `Manually backup wr's job database.

The manager automatically backs up its database to the configured location every
time there is a change.

You can use this command to create an additional backup to a different location.
Note that the manager must be running.

(When the manager is stopped, you can backup the database by simply copying it
somewhere.)`,
	Run: func(cmd *cobra.Command, args []string) {
		if backupPath == "" {
			die("--path is required")
		}
		timeout := time.Duration(timeoutint) * time.Second

		jq := connect(timeout)
		defer func() {
			err := jq.Disconnect()
			if err != nil {
				warn("Disconnecting from the server failed: %s", err)
			}
		}()

		err := jq.BackupDB(backupPath)
		if err != nil {
			die("%s", err)
		}
	},
}

// reportLiveStatus is used by the status command on a working connection to
// distinguish between the server being in a normal 'started' state or the
// 'drain' state.
func reportLiveStatus(jq *jobqueue.Client) {
	fmt.Println(jq.ServerInfo.Mode)
}

func init() {
	ctx := context.Background()
	defaultMaxRAM, err := internal.ProcMeminfoMBs()
	if err != nil {
		defaultMaxRAM = 0
	}

	RootCmd.AddCommand(managerCmd)
	managerCmd.AddCommand(managerStartCmd)
	managerCmd.AddCommand(managerPauseCmd)
	managerCmd.AddCommand(managerResumeCmd)
	managerCmd.AddCommand(managerDrainCmd)
	managerCmd.AddCommand(managerStopCmd)
	managerCmd.AddCommand(managerStatusCmd)
	managerCmd.AddCommand(managerBackupCmd)

	// flags specific to these sub-commands
	defaultConfig := internal.DefaultConfig(ctx)
	managerStartCmd.Flags().BoolVarP(&foreground, "foreground", "f", false, "do not daemonize")
	managerStartCmd.Flags().StringVarP(&scheduler, "scheduler", "s", defaultConfig.ManagerScheduler, "['local','lsf','openstack'] job scheduler")
	managerStartCmd.Flags().IntVarP(&managerTimeoutSeconds, "timeout", "t", 10, "how long to wait in seconds for the manager to start up")
	managerStartCmd.Flags().IntVar(&maxLocalCores, "max_cores", runtime.NumCPU(), "maximum number of local cores to use to run cmds; -1 means unlimited, 0 allows only 0-core jobs")
	managerStartCmd.Flags().IntVar(&maxLocalRAM, "max_ram", defaultMaxRAM, "maximum MB of local memory to use to run cmds; -1 means unlimited, 0 prevents jobs running locally")
	managerStartCmd.Flags().IntVar(&cloudSpawns, "cloud_spawns", defaultConfig.CloudSpawns, "for cloud schedulers, maximum number of simultaneous server spawns during scale-up")
	managerStartCmd.Flags().StringVarP(&osPrefix, "cloud_os", "o", defaultConfig.CloudOS, "for cloud schedulers, prefix name of the OS image your servers should use")
	managerStartCmd.Flags().StringVarP(&osUsername, "cloud_username", "u", defaultConfig.CloudUser, "for cloud schedulers, username needed to log in to the OS image specified by --cloud_os")
	managerStartCmd.Flags().StringVar(&localUsername, "local_username", realUsername(), fmt.Sprintf("for cloud schedulers, your local username outside of the cloud (max length %d)", maxCloudResourceUsernameLength))
	managerStartCmd.Flags().IntVarP(&osRAM, "cloud_ram", "r", defaultConfig.CloudRAM, "for cloud schedulers, ram (MB) needed by the OS image specified by --cloud_os")
	managerStartCmd.Flags().IntVarP(&osDisk, "cloud_disk", "d", defaultConfig.CloudDisk, "for cloud schedulers, minimum disk (GB) for servers")
	managerStartCmd.Flags().StringVarP(&flavorRegex, "cloud_flavor", "l", defaultConfig.CloudFlavor, "for cloud schedulers, a regular expression to limit server flavors that can be automatically picked")
	managerStartCmd.Flags().StringVar(&flavorSets, "cloud_flavor_sets", defaultConfig.CloudFlavorSets, "for cloud schedulers, sets of flavors assigned to different hardware, in the form f1,f2;f3,f4")
	managerStartCmd.Flags().StringVarP(&postCreationScript, "cloud_script", "p", defaultConfig.CloudScript, "for cloud schedulers, path to a start-up script that will be run on each server created")
	managerStartCmd.Flags().StringVarP(&kubeNamespace, "namespace", "", "", "for the kubernetes scheduler, the namespace to use")
	managerStartCmd.Flags().StringVarP(&configMapName, "config_map", "", "", "for the kubernetes scheduler, provide an existing config map to initialise all pods with. To be used instead of --cloud_script")
	managerStartCmd.Flags().IntVarP(&serverKeepAlive, "cloud_keepalive", "k", defaultConfig.CloudKeepAlive, "for cloud schedulers, how long in seconds to keep idle spawned servers alive for; 0 means forever")
	managerStartCmd.Flags().IntVar(&cloudServersAutoConfirmDead, "cloud_auto_confirm_dead", defaultConfig.CloudAutoConfirmDead, "for cloud schedulers, how long to wait in minutes before destroying bad servers; 0 means forever")
	managerStartCmd.Flags().IntVarP(&maxServers, "cloud_servers", "m", defaultConfig.CloudServers, "for cloud schedulers, maximum number of additional servers to spawn; -1 means unlimited")
	managerStartCmd.Flags().StringVar(&cloudCIDR, "cloud_cidr", defaultConfig.CloudCIDR, "for cloud schedulers, CIDR of the subnet to spawn servers in")
	managerStartCmd.Flags().BoolVar(&cloudUseConfigDrive, "cloud_use_config_drives", false, "for cloud schedulers, spawn servers with configuration drives")
	managerStartCmd.Flags().BoolVar(&cloudNoSecurityGroups, "cloud_disable_security_groups", false, "for cloud schedulers, disable the use of security groups on spawned servers")
	managerStartCmd.Flags().StringVar(&cloudConfigFiles, "cloud_config_files", defaultConfig.CloudConfigFiles, "for cloud schedulers, comma separated paths of config files to copy to spawned servers")
	managerStartCmd.Flags().BoolVar(&setDomainIP, "set_domain_ip", defaultConfig.ManagerSetDomainIP, "on success, use infoblox to set your domain's IP")
	managerStartCmd.Flags().BoolVar(&useCertDomain, "use_cert_domain", false, "if cert domain is configured, provide it to spawned clients instead of our IP address")
	managerStartCmd.Flags().BoolVar(&managerDebug, "debug", false, "include extra debugging information in the logs")
	managerStartCmd.Flags().BoolVar(&runnerDebug, "runner_debug", false, "have runners log to syslog on their machines")

	managerBackupCmd.Flags().StringVarP(&backupPath, "path", "p", "", "backup file path")
}

func logStarted(s *jobqueue.ServerInfo, token []byte) {
	info("wr manager %s started on %s, pid %d", jobqueue.ServerVersion, sAddr(s), s.PID)

	// go back to just stderr so we don't log token to file (this doesn't affect
	// server logging)
	clog.ToDefaultAtLevel("info")
	info("wr's web interface can be reached at https://%s:%s/?token=%s", s.Host, s.WebPort, string(token))

	if setDomainIP {
		ip, err := internal.CurrentIP("")
		if err != nil {
			warn("could not get IP address of localhost: %s", err)
		}
		err = internal.InfobloxSetDomainIP(s.Host, ip)
		if err != nil {
			warn("failed to set domain IP: %s", err)
		} else {
			info("set IP of %s to %s", s.Host, ip)
		}
	}
}

func startJQ(ctx context.Context, postCreation []byte) {
	if runtime.NumCPU() == 1 {
		// we might lock up with only 1 proc if we mount
		runtime.GOMAXPROCS(2)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	// change the logger to log to both STDERR and our configured log file;
	// we also create a new logger for internal use by the server later
	clog.ToDefault()

	logLevel := "warn"

	if managerDebug {
		logLevel = "debug"
	}

	fileHandler, err := clog.CreateFileHandlerAtLevel(config.ManagerLogFile, logLevel)

	if err != nil {
		warn("wr manager could not log to %s: %s", config.ManagerLogFile, err)
	}

	clog.AddHandler(fileHandler)

	// we will spawn runners, which means we need to know the path to ourselves
	// in case we're not in the user's $PATH
	exe, err := osext.Executable()
	if err != nil {
		die("wr manager failed to start : %s\n", err)
	}

	var schedulerConfig interface{}
	serverCIDR := ""
	switch scheduler {
	case "local":
		schedulerConfig = &jqs.ConfigLocal{
			Shell:    config.RunnerExecShell,
			MaxCores: maxLocalCores,
			MaxRAM:   maxLocalRAM,
		}
	case "lsf":
		schedulerConfig = &jqs.ConfigLSF{
			Deployment:     config.Deployment,
			Shell:          config.RunnerExecShell,
			PrivateKeyPath: config.PrivateKeyPath,
		}
	case "openstack":
		mport, errf := strconv.Atoi(config.ManagerPort)
		if errf != nil {
			die("wr manager failed to start : %s\n", errf)
		}

		var serverPorts []int
		if !cloudNoSecurityGroups {
			serverPorts = []int{22, mport}
		}

		schedulerConfig = &jqs.ConfigOpenStack{
			ResourceName:         cloudResourceName(localUsername),
			SavePath:             filepath.Join(config.ManagerDir, "cloud_resources.openstack"),
			ServerPorts:          serverPorts,
			UseConfigDrive:       cloudUseConfigDrive,
			OSPrefix:             osPrefix,
			OSUser:               osUsername,
			OSRAM:                osRAM,
			OSDisk:               osDisk,
			FlavorRegex:          flavorRegex,
			FlavorSets:           flavorSets,
			PostCreationScript:   postCreation,
			ConfigFiles:          cloudConfigFiles,
			ServerKeepTime:       time.Duration(serverKeepAlive) * time.Second,
			StateUpdateFrequency: 1 * time.Minute,
			MaxInstances:         maxServers,
			SimultaneousSpawns:   cloudSpawns,
			MaxLocalCores:        &maxLocalCores,
			MaxLocalRAM:          &maxLocalRAM,
			Shell:                config.RunnerExecShell,
			CIDR:                 cloudCIDR,
			Umask:                config.ManagerUmask,
		}
		serverCIDR = cloudCIDR
	case kubernetes:
		schedulerConfig = &jqs.ConfigKubernetes{
			Image:              osPrefix,
			PostCreationScript: postCreation,
			ConfigMap:          configMapName,
			ConfigFiles:        cloudConfigFiles,
			Shell:              config.RunnerExecShell,
			TempMountPath:      filepath.Dir(exe) + "/",
			LocalBinaryPath:    exe,
			Namespace:          kubeNamespace,
			ManagerDir:         config.ManagerDir,
			Debug:              managerDebug,
		}

	}

	if cloudConfig, ok := schedulerConfig.(jqs.CloudConfig); ok {
		// this is a cloud scheduler, so include our ca.pem and client.token
		// files in ConfigFiles, so that they will be copied to all servers
		// that get created.
		cloudConfig.AddConfigFile(config.ManagerTokenFile + ":~/.wr_" + config.Deployment + "/client.token")
		if config.ManagerCAFile != "" {
			cloudConfig.AddConfigFile(config.ManagerCAFile + ":~/.wr_" + config.Deployment + "/ca.pem")
		}

		if scheduler != kubernetes {
			// also check that we're actually in the cloud, or this is not going to
			// work
			provider, errc := cloud.New(ctx, scheduler, cloudResourceName(localUsername),
				filepath.Join(config.ManagerDir, "cloud_resources."+scheduler))
			if errc != nil {
				die("could not connect to %s: %s", scheduler, errc)
			}
			if !provider.InCloud() {
				die("according to hostname, this is not an instance in %s", scheduler)
			}
		} else {
			// kubernetes specific code to check if we are in a wr pod inside a cluster
			kubeWRPod := client.InWRPod()
			if !kubeWRPod {
				die("according to hostname and env vars, this is not a container in kubernetes")
			}
		}
	}

	runnerCmd := exe + " runner -s '%s' --deployment %s --server '%s' --domain %s -r %d -m %d"
	if runnerDebug {
		runnerCmd += " --debug"
	}

	var wgDebug strings.Builder
	deadlockBuf := new(bytes.Buffer)
	sync.Opts.LogBuf = deadlockBuf
	sync.Opts.DeadlockTimeout = deadlockTimeout
	sync.Opts.OnPotentialDeadlock = func() {
		wgMsg := wgDebug.String()
		if wgMsg != "" {
			clog.Warn(ctx, "waitgroups waiting", "msgs", wgMsg)
			wgDebug.Reset()
		}

		clog.Crit(ctx, "deadlock", "err", deadlockBuf.String())
	}
	waitgroup.Opts.Logger = &wgDebug
	waitgroup.Opts.Disable = false

	// start the jobqueue server
	server, msg, token, err := jobqueue.Serve(ctx, jobqueue.ServerConfig{
		Port:            config.ManagerPort,
		WebPort:         config.ManagerWeb,
		SchedulerName:   scheduler,
		SchedulerConfig: schedulerConfig,
		RunnerCmd:       runnerCmd,
		DBFile:          config.ManagerDBFile,
		DBFileBackup:    config.ManagerDBBkFile,
		TokenFile:       config.ManagerTokenFile,
		UploadDir:       config.ManagerUploadDir,
		CAFile:          config.ManagerCAFile,
		CertFile:        config.ManagerCertFile,
		KeyFile:         config.ManagerKeyFile,
		CertDomain:      config.ManagerCertDomain,
		DomainMatchesIP: useCertDomain,
		AutoConfirmDead: time.Duration(cloudServersAutoConfirmDead) * time.Minute,
		Deployment:      config.Deployment,
		CIDR:            serverCIDR,
	})

	if msg != "" {
		info("wr manager : %s", msg)
	}

	if err != nil {
		die("wr manager failed to start : %s", err)
	}

	logStarted(server.ServerInfo, token)

	fileHandler, err = clog.CreateFileHandlerAtLevel(config.ManagerLogFile, logLevel)
	if err != nil {
		warn("wr manager could not log to %s: %s", config.ManagerLogFile, err)
	}

	clog.AddHandler(fileHandler)

	// block forever while the jobqueue does its work
	err = server.Block()

	wgMsg := wgDebug.String()
	if wgMsg != "" {
		clog.Warn(ctx, "waitgroups waiting", "msgs", wgMsg)
	}

	if err != nil {
		saddr := sAddr(server.ServerInfo)
		jqerr, ok := err.(jobqueue.Error)
		switch {
		case ok && jqerr.Err == jobqueue.ErrClosedTerm:
			info("wr manager on %s gracefully stopped (received SIGTERM)", saddr)
		case ok && jqerr.Err == jobqueue.ErrClosedInt:
			info("wr manager on %s gracefully stopped (received SIGINT)", saddr)
		case ok && jqerr.Err == jobqueue.ErrClosedStop:
			info("wr manager on %s gracefully stopped (following a drain)", saddr)
		default:
			warn("wr manager on %s exited unexpectedly: %s", saddr, err)
		}
	}
}

// deleteToken should be called on successful, known clean stop of the manager,
// so that the next time the manager is started it will create a new token.
// For un-clean exits of the manager, we should keep the token so the manager
// re-uses it, allowing any runners to reconnect.
func deleteToken() {
	err := os.Remove(config.ManagerTokenFile)
	if err != nil && !os.IsNotExist(err) {
		warn("could not remove token file [%s]: %s", config.ManagerTokenFile, err)
	}
}
