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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/fatih/color"
	"github.com/kardianos/osext"
	"github.com/spf13/cobra"
)

// cloudBinDir is where we will upload executables to our created cloud server;
// it needs to be somewhere that is likely to be writable on all OS images, and
// in particular not in the home dir since we may want to run commands on
// spawned servers that are running different OS images with different user.
const cloudBinDir = "/tmp"

// wrConfigFileName is the name of our main config file, which we need when we
// create on our created cloud server
const wrConfigFileName = ".wr_config.yml"

// wrEnvFileName is the name of our environment variables file, which we need
// when we start the manager on our created cloud server
const wrEnvFileName = ".wr_envvars"

// options for this cmd
var providerName string
var cloudMaxServers int
var serverKeepAlive int
var osPrefix string
var osUsername string
var osRAM int
var osDisk int
var flavorRegex string
var managerFlavor string
var flavorSets string
var postCreationScript string
var postDeploymentScript string
var cloudSpawns int
var cloudGatewayIP string
var cloudCIDR string
var cloudDNS string
var cloudConfigFiles string
var forceTearDown bool
var setDomainIP bool
var cloudDebug bool
var cloudManagerTimeoutSeconds int
var cloudResourceNameUniquer string
var maxManagerCores int
var maxManagerRAM int
var cloudServersAll bool
var cloudServerID string
var cloudServersConfirmDead bool
var cloudServersAutoConfirmDead int

// cloudCmd represents the cloud command
var cloudCmd = &cobra.Command{
	Use:   "cloud",
	Short: "Cloud infrastructure creation",
	Long: `Cloud infrastructure creation.

To run wr in the cloud, you need to create at least 1 cloud server with certain
ports open so that you can start running "wr manager" on it. From there the
manager will run your commands on additional servers spawned as demand dictates.

The cloud sub-commands make it easy to get started, interact with that remote
manager, and clean up afterwards.`,
}

// deploy sub-command brings up a "head" node in the cloud and starts a proxy
// daemon to interact with the manager we spawn there
var cloudDeployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy a manager to a cloud server",
	Long: `Start up 'wr manager' on a cloud server.

Deploy creates all the necessary cloud resources (networks, keys, security
profiles etc.) and starts a cloud server, on which 'wr manager' is run. In a
production deployment the remote manager will use a copy of the latest version
of the wr database, taken from your S3 db backup location, or if you don't use
S3, from your local filesystem.

Deploy then sets up ssh forwarding in the background that lets you use the
normal wr command line utilities such as 'wr add' and view the wr website
locally, even though the manager is actually running remotely. Note that this
precludes starting wr manager locally as well. Also be aware that while 'wr add'
normally associates your current environment variables and working directory
with the cmds you want to run, with a remote deployment the working directory
defaults to /tmp, and commands will be run with the non-login environment
variables of the server the command is run on.

The --script option value can be, for example, the path to a bash script that
you want to run on any created cloud server before any commands run on them. You
might install some software for example. Note that the script is run as the user
defined by --username; if necessary, your bash script may have to prefix its
commands with 'sudo' if the command would only work as root user. Also, there is
a time limit of 15 mins for the script to run. If you're installing lots of
software, consider creating a new image instead, and using the --os option.

The --config_files option lets you specify comma separated arbitrary text file
paths that should be copied from your local system to any created cloud servers.
Absolute paths will be copied to the same absolute path on the server. For files
that should be transferred from your home directory to the cloud server's home
directory (which could be at different absolute paths), prefix your path with
"~/". If the local path of a file is unrelated to the remote path, separate the
paths with a colon to specify source and destination, eg.
"~/projectSpecific/.s3cfg:~/.s3cfg".
Local paths that don't exist are silently ignored.
This option is important if you want to be able to queue up commands that rely
on the --mounts option to 'wr add': you'd specify your s3 config file(s) which
contain your credentials for connecting to your s3 bucket(s).

The --max_local_cores and --max_local_memory options specify how much of the
cloud server that runs wr manager itself should also be used to run commands.
To have an uncontended manager, you could set --max_local_memory to 0, and wr
will run all commands on additional spawned cloud servers. If you don't set
--max_local_memory to 0, but do set --max_local_cores to 0, up to four 0-core
jobs will be allowed to run on the manager's server, but nothing else will. This
is useful if you don't want to waste resources or spend time spawning servers to
run trivial commands that complete near instantly.

The --on_success optional value is the path to some executable that you want to
run locally after the deployment is successful. The executable will be run with
the environment variables WR_MANAGERIP and WR_MANAGERCERTDOMAIN set to the IP
address of the cloud server that wr manager was started on and the domain you
configured as valid for your own TLS certificate respectively. Your executable
might be, for example, a bash script that updates your local DNS entries.

The --set_domain_ip option is an alternative to --on_success if your DNS is
managaged with infoblox. You will need the environment variables INFOBLOX_HOST,
INFOBLOX_USER and INFOBLOX_PASS set for this option to work. After a successful
deployment infoblox will be used to first delete all A records for your
configured WR_MANAGERCERTDOMAIN (which can't be localhost), and then create an
A record for WR_MANAGERCERTDOMAIN that points to the IP address of the cloud
server that wr manager was started on.

The --flavor_sets option lets you describe how flavors relate to hardware in
your cloud setup, and is probably only relevant to private clouds such as
OpenStack, where hardware is limited. If some flavors can only be created on a
subset of your hardware, and other flavors can only be created on a different
subset of your hardware, then you should describe those flavors as being in
different sets, using the form f1,f2;f3,f4 where f1 and f2 are in the same set
and f3 and f4 are in a different set (these names are treated as regular
expressions). Doing this will result in the following behaviour: a flavor in the
first set in your list (that matches your --flavor regex and is suitable for the
job) will be picked, but if a server of that flavor can't be created due to lack
of hardware, then a flavor from the next set in your list will be picked and
tried instead. If there is a lack of hardware in all sets, the first is tried
again.

Deploy can work with any given --os OS image because it uploads wr to any server
it creates; your OS image does not have to have wr installed on it. The only
requirements of the OS image are that it support ssh and sftp on port 22, and
that it be a 64bit linux-like system with /proc/*/smaps, /tmp and some local
writeable disk space in the home directory. For --mounts to work, fuse-utils
must be installed, and /etc/fuse.conf should already have user_allow_other set
or at least be present and commented out (wr will enable it).

The openstack provider needs these environment variables to be set:
OS_AUTH_URL, OS_USERNAME, OS_PASSWORD and OS_REGION_NAME.
You will need additional environment variables, but these depend on the version
of OpenStack you're using. Older installs may need:
OS_TENANT_ID, OS_TENANT_NAME
Newer installs may need:
OS_PROJECT_ID or OS_PROJECT_NAME, and one of OS_DOMAIN_ID (aka
OS_PROJECT_DOMAIN_ID) or OS_DOMAIN_NAME (aka OS_USER_DOMAIN_NAME)
Depending on the install, one of OS_TENANT_ID and OS_PROJECT_ID may be required.
You might also need OS_USERID.
You can get the necessary values by logging in to your OpenStack dashboard web
interface and looking for the 'Download Openstack RC File' button. For older
installs this is in the Compute -> Access & Security, 'API Access' tab. For
newer installs it is under Project -> API Access.
Finally, you may need to add to that RC file OS_POOL_NAME to define the name
of the network to get floating IPs from (for older installs this defaults to
"nova", for newer ones it defaults to "public").
If you're concerned about security, you can immediately 'unset OS_PASSWORD'
after doing a deploy. (You'll need to set it again before doing a teardown.)

Note that when specifying the OpenStack environment variable 'OS_AUTH_URL', it
must work from within an OpenStack server running your chosen OS image. For
http:// urls, this is most likely to succeed if you use an IP address instead of
a domain name. For https:// urls you'll need a domain name, and will have to
ask your administrator for the appropriate --network_dns settings (or clouddns
config option) to use; the DNS must be able to resolve the domain name from
within OpenStack.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if providerName == "" {
			die("--provider is required")
		}
		if osPrefix == "" {
			die("--os is required")
		}
		if osUsername == "" {
			die("--username is required")
		}

		var postCreation []byte
		if postCreationScript != "" {
			var err error
			postCreation, err = os.ReadFile(postCreationScript)
			if err != nil {
				die("--script %s could not be read: %s", postCreationScript, err)
			}
		}

		if len(cloudResourceNameUniquer) > maxCloudResourceUsernameLength {
			die("--resource_name must be %d characters or less", maxCloudResourceUsernameLength)
		}

		// first we need our working directory to exist
		createWorkingDir()

		// check to see if the manager is already running (regardless of the
		// state of the pid file); we can't proxy if a manager is already up
		jq := connect(1*time.Second, true)
		if jq != nil {
			die("wr manager on port %s is already running (pid %d); please stop it before trying again.", config.ManagerPort, jq.ServerInfo.PID)
		}

		// we will spawn wr on the remote server we will create, which means we
		// need to know the path to ourselves in case we're not in the user's
		// $PATH
		exe, err := osext.Executable()
		if err != nil {
			die("could not get the path to wr: %s", err)
		}

		// later we will copy our server cert and key to the cloud "head node";
		// if we don't have any, generate them now
		err = internal.CheckCerts(config.ManagerCertFile, config.ManagerKeyFile)
		if err != nil {
			err = internal.GenerateCerts(config.ManagerCAFile, config.ManagerCertFile, config.ManagerKeyFile, config.ManagerCertDomain)
			if err != nil {
				die("could not generate certs: %s", err)
			}
			info("created a new key and certificate for TLS")
		}

		// for debug purposes, set up logging to STDERR
		setupLogging(kubeDebug)

		// get all necessary cloud resources in place
		mp, err := strconv.Atoi(config.ManagerPort)
		if err != nil {
			die("bad manager_port [%s]: %s", config.ManagerPort, err)
		}
		wp, err := strconv.Atoi(config.ManagerWeb)
		if err != nil {
			die("bad manager_web [%s]: %s", config.ManagerWeb, err)
		}
		provider, err := cloud.New(ctx, providerName, cloudResourceName(cloudResourceNameUniquer),
			filepath.Join(config.ManagerDir, "cloud_resources."+providerName))
		if err != nil {
			die("failed to connect to %s: %s", providerName, err)
		}
		info("please wait while %s resources are created...", providerName)
		err = provider.Deploy(ctx, &cloud.DeployConfig{
			RequiredPorts:  []int{22, mp, wp},
			GatewayIP:      cloudGatewayIP,
			CIDR:           cloudCIDR,
			DNSNameServers: strings.Split(cloudDNS, ","),
		})
		if err != nil {
			die("failed to create resources in %s: %s", providerName, err)
		}

		// get/spawn a "head node" server
		keyPath := filepath.Join(config.ManagerDir, "cloud_resources."+providerName+".key")
		fmPidPath := filepath.Join(config.ManagerDir, "cloud_resources."+providerName+".fm.pid")
		fwPidPath := filepath.Join(config.ManagerDir, "cloud_resources."+providerName+".fw.pid")
		var server *cloud.Server
		usingExistingServer := false
		alreadyUp := false
		servers := provider.Servers()
		for _, thisServer := range servers {
			if thisServer.Alive(ctx) {
				usingExistingServer = true
				server = thisServer
				info("using existing %s server at %s", providerName, server.IP)

				// see if this server is already running wr manager by trying
				// to re-establish our port forwarding, which may have failed
				// due to temporary networking issues
				err = startForwarding(server.IP, osUsername, keyPath, mp, fmPidPath)
				if err != nil {
					warn("could not reestablish port forwarding to server %s, port %d", server.IP, mp)
				}
				err = startForwarding(server.IP, osUsername, keyPath, wp, fwPidPath)
				if err != nil {
					warn("could not reestablish port forwarding to server %s, port %d", server.IP, wp)
				}
				jq = connect(2*time.Second, true)
				if jq != nil {
					info("reconnected to existing wr manager on %s", sAddr(jq.ServerInfo))
					alreadyUp = true
				} else {
					// clean up any existing or partially failed forwarding
					pid, running := checkProcess(fmPidPath)
					if running {
						errk := killProcess(pid)
						if errk != nil {
							warn("failed to kill ssh forwarder pid %d", pid)
						}
					}
					errr := os.Remove(fmPidPath)
					if errr != nil && !os.IsNotExist(errr) {
						warn("failed to remove forwarder pid file %s: %s", fmPidPath, errr)
					}
					pid, running = checkProcess(fwPidPath)
					if running {
						errk := killProcess(pid)
						if errk != nil {
							warn("failed to kill ssh forwarder pid %d", pid)
						}
					}
					errr = os.Remove(fwPidPath)
					if errr != nil && !os.IsNotExist(errr) {
						warn("failed to remove forwarder pid file %s: %s", fwPidPath, errr)
					}
				}

				break
			}
		}
		if server == nil {
			info("please wait while a server is spawned on %s...", providerName)
			if managerFlavor == "" {
				managerFlavor = flavorRegex
			}
			flavor, errf := provider.CheapestServerFlavor(ctx, 1, osRAM, managerFlavor)
			if errf != nil {
				teardown(ctx, provider)
				die("failed to launch a server in %s: %s", providerName, errf)
			}
			server, errf = provider.Spawn(ctx, osPrefix, osUsername, flavor.ID, osDisk, 0*time.Second, true)
			if errf != nil {
				teardown(ctx, provider)
				die("failed to launch a server in %s: %s", providerName, errf)
			}
			errf = server.WaitUntilReady(context.Background(), cloudConfigFiles, postCreation)
			if errf != nil {
				teardown(ctx, provider)
				die("failed to launch a server in %s: %s", providerName, errf)
			}
		}

		if setDomainIP {
			err = internal.InfobloxSetDomainIP(config.ManagerCertDomain, server.IP)
			if err != nil {
				warn("failed to set domain IP: %s", err)
				setDomainIP = false
			} else {
				info("set IP of %s to %s", config.ManagerCertDomain, server.IP)
			}
		}

		// ssh to the server, copy over our exe, and start running wr manager
		// there
		if !alreadyUp {
			info("please wait while I start 'wr manager' on the %s server at %s...", providerName, server.IP)
			bootstrapOnRemote(provider, server, exe, mp, wp, keyPath, usingExistingServer, setDomainIP)

			// rather than daemonize and use a go ssh forwarding library or
			// implement myself using the net package, since I couldn't get them
			// to work reliably and completely, we'll just spawn ssh -L in the
			// background and keep note of the pids so we can kill them during
			// teardown
			err = startForwarding(server.IP, osUsername, keyPath, mp, fmPidPath)
			if err != nil {
				teardown(ctx, provider)
				die("failed to set up port forwarding to %s:%d: %s", server.IP, mp, err)
			}
			err = startForwarding(server.IP, osUsername, keyPath, wp, fwPidPath)
			if err != nil {
				teardown(ctx, provider)
				die("failed to set up port forwarding to %s:%d: %s", server.IP, wp, err)
			}

			// check that we can now connect to the remote manager
			jq = connect(40*time.Second, true)
			if jq == nil {
				teardown(ctx, provider)
				die("could not talk to wr manager on server at %s after 40s", server.IP)
			}

			info("wr manager remotely started on %s", sAddr(jq.ServerInfo))
		}

		info("should you need to, you can ssh to this server using `ssh -i %s %s@%s`", keyPath, osUsername, server.IP)
		token, err := token()
		if err != nil {
			warn("token could not be read! [%s]", err)
		}
		info("wr's web interface can be reached locally at https://%s:%s/?token=%s", jq.ServerInfo.Host, jq.ServerInfo.WebPort, string(token))

		if postDeploymentScript != "" {
			cmd := exec.Command(postDeploymentScript) // #nosec
			cmd.Env = append(os.Environ(), "WR_MANAGERIP="+server.IP, "WR_MANAGERCERTDOMAIN="+jq.ServerInfo.Host)
			err = cmd.Run()
			if err != nil {
				warn("--on_success executable [%s] failed: %s", postDeploymentScript, err)
			}
		}
	},
}

// teardown sub-command deletes all cloud resources we created and then stops
// the daemon by sending it a term signal
var cloudTearDownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Delete all cloud resources that deploy created",
	Long: `Immediately stop the remote workflow manager, saving its state.

Deletes all cloud resources that wr created (servers, networks, keys, security
profiles etc.). (Except for any files that were saved to persistent cloud
storage.)

Note that any runners that are currently running will die, along with any
commands they were running. It is more graceful to issue 'wr manager drain'
first, and regularly rerun drain until it reports the manager is stopped, and
only then request a teardown (you'll need to add the --force option). But this
is only a good idea if you have configured wr to back up its database to S3, as
otherwise your database going forward will not reflect anything you did during
that cloud deployment.

If you don't back up to S3, the teardown command tries to copy the remote
database locally, which is only possible while the remote server is still up
and accessible.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if providerName == "" {
			die("--provider is required")
		}

		// before stopping the manager, make sure we can interact with the
		// provider - that our credentials are correct
		setupLogging(kubeDebug)

		provider, err := cloud.New(ctx, providerName, cloudResourceName(cloudResourceNameUniquer),
			filepath.Join(config.ManagerDir, "cloud_resources."+providerName))
		if err != nil {
			die("failed to connect to %s: %s", providerName, err)
		}
		headNode := provider.HeadNode()
		var headNodeKnown bool
		if headNode != nil {
			headNodeKnown = headNode.Known(ctx)
		}

		// now check if the ssh forwarding is up
		fmPidFile := filepath.Join(config.ManagerDir, "cloud_resources."+providerName+".fm.pid")
		fmPid, fmRunning := checkProcess(fmPidFile)

		// try and stop the remote manager
		noManagerMsg := "; deploy first or use --force option"
		noManagerForcedMsg := "; tearing down anyway - you may lose changes if not backing up the database to S3!"
		serverHadProblems := false
		if fmRunning {
			jq := connect(1*time.Second, true)
			if jq != nil {
				if !headNodeKnown {
					die("was able to connect to the manager, but the deployed node is reported as non-existent; are you using the same credentials you deployed this manager with?")
				}

				var syncMsg string
				if internal.IsRemote(config.ManagerDBBkFile) {
					if _, errf := os.Stat(config.ManagerDBFile); !os.IsNotExist(errf) {
						// move aside the local database so that if the manager is
						// started locally, the database will be restored from S3
						// and have the history of what was run in the cloud
						if errf = os.Rename(config.ManagerDBFile, config.ManagerDBFile+".old"); err == nil {
							syncMsg = "; the local database will be updated from S3 if manager started locally"
						} else {
							warn("could not rename the local database; if the manager is started locally, it will not be updated with the latest changes in S3! %s", errf)
						}
					}
				} else {
					// copy the remote database locally, so if the manager is
					// started locally we have the history of what was run in
					// the cloud. The gap between backing up and shutting down
					// is "fine"; though some db writes may occur, the user
					// obviously doesn't care about them. On recovery we won't
					// break any pipelines.
					errf := jq.BackupDB(config.ManagerDBFile)
					if errf != nil {
						msg := "there was an error trying to sync the remote database: " + errf.Error()
						if forceTearDown {
							warn(msg + noManagerForcedMsg)
						} else {
							die(msg)
						}
					}
					syncMsg = " and local database updated"
				}

				ok := jq.ShutdownServer()
				if ok {
					info("the remote wr manager was shut down" + syncMsg)
				} else {
					msg := "there was an error trying to shut down the remote wr manager"
					if forceTearDown {
						warn(msg + noManagerForcedMsg)
						serverHadProblems = true
					} else {
						die(msg)
					}
				}
			} else {
				msg := "the remote wr manager could not be connected to in order to shut it down"
				if forceTearDown {
					warn(msg + noManagerForcedMsg)
					serverHadProblems = true
				} else {
					die(msg + noManagerMsg)
				}
			}
		} else {
			if forceTearDown {
				warn("the deploy port forwarding is not running, so the remote manager could not be stopped" + noManagerForcedMsg)
				serverHadProblems = true
			} else {
				die("the deploy port forwarding is not running, so can't safely teardown" + noManagerMsg)
			}
		}

		// copy over any manager logs that got created locally (ignore errors,
		// and overwrite any existing file) *** currently missing the final
		// shutdown message doing things this way, but ok?...
		if headNodeKnown && headNode.Alive(ctx) {
			cloudLogFilePath := config.ManagerLogFile + "." + providerName
			errf := headNode.DownloadFile(context.Background(), filepath.Join("./.wr_"+config.Deployment, "log"), cloudLogFilePath)

			if errf != nil {
				warn("could not download the remote log file: %s", errf)
			} else {
				// display any crit lines in that log file
				if errf == nil {
					f, errf := os.Open(cloudLogFilePath)
					if errf == nil {
						explained := false
						scanner := bufio.NewScanner(f)
						for scanner.Scan() {
							line := scanner.Text()
							if strings.Contains(line, "lvl=crit") {
								if !explained {
									warn("looks like the manager on the remote server suffered critical errors:")
									explained = true
								}
								fmt.Println(line)
							}
						}

						if serverHadProblems {
							info("the remote manager log has been saved to %s", cloudLogFilePath)
						}
					}
				}
			}
		}

		// teardown cloud resources we created
		err = provider.TearDown(ctx)
		if err != nil {
			die("failed to delete the cloud resources previously created: %s", err)
		}
		err = os.Remove(filepath.Join(config.ManagerDir, "cloud_resources."+providerName+".key"))
		if err != nil {
			warn("failed to delete the cloud resources file: %s", err)
		}
		info("deleted all cloud resources previously created")

		err = os.Remove(config.ManagerTokenFile)
		if err != nil {
			warn("failed to delete the token file: %s", err)
		}

		// kill the ssh forwarders
		if fmRunning {
			err = killProcess(fmPid)
			if err == nil {
				err = os.Remove(fmPidFile)
				if err != nil && !os.IsNotExist(err) {
					warn("failed to remove the forwarder pid file %s: %s", fmPidFile, err)
				}
			}
		}
		fwPidFile := filepath.Join(config.ManagerDir, "cloud_resources."+providerName+".fw.pid")
		if fwPid, fwRunning := checkProcess(fwPidFile); fwRunning {
			err = killProcess(fwPid)
			if err == nil {
				err = os.Remove(fwPidFile)
				if err != nil && !os.IsNotExist(err) {
					warn("failed to remove the forwarder pid file %s: %s", fwPidFile, err)
				}
			}
		}
	},
}

// servers sub-command currently lets you view "server might be dead" warnings,
// and to confirm that they're dead.
var cloudServersCmd = &cobra.Command{
	Use:   "servers",
	Short: "View cloud servers that might be dead and confirm if they're dead",
	Long: `View cloud servers that might be dead and confirm if they're dead.

When running your workflow jobs, the manager may spawn cloud servers on which
the jobs can be run. If these become unresponsive, this command can be used to
see which have issues.

After investigating the servers with the appropriate tools you have that can
interegate your cloud, if the servers are definitely never going to be useable
(eg. they have been destroyed and no longer exist, or have had kernel panics),
you can use this command to confirm they are dead with --confirmdead, which
means the manager will no longer think those server resources are in use. If the
servers still exist, confirming they are dead will result in the manager trying
to destroy them.

If --confirmdead results in wr successfully destroying servers or confirming
they no longer exist, any jobs that were running on those servers (which most
likely would have reached "lost contact" status) will be killed or confirmed
dead, so that they will either become buried or retry according to their
configured number of retries. If jobs hadn't yet reached  "lost contact" status,
they will have a status of running until the time they would normally become
lost, at which point they will automatically be confirmed dead.`,
	Run: func(cmd *cobra.Command, args []string) {
		if cloudServersConfirmDead && (!cloudServersAll && cloudServerID == "") {
			die("in --confirmdead mode, either --all or --identifier must be specified")
		}

		jq := connect(time.Duration(timeoutint)*time.Second, false)
		defer func() {
			err := jq.Disconnect()
			if err != nil {
				warn("failed to disconnect: %s", err)
			}
		}()

		var badServers []*jobqueue.BadServer
		var killedJobs []*jobqueue.Job
		var err error
		if cloudServersConfirmDead {
			badServers, killedJobs, err = jq.ConfirmCloudServersDead(cloudServerID)
			if err != nil {
				die("%s", err)
			}

			if len(badServers) == 0 {
				info("no cloud servers were eligible to be confirmed dead")
				return
			}
			info("these cloud servers were confirmed dead:")
		} else {
			badServers, err = jq.GetBadCloudServers()
			if err != nil {
				die("%s", err)
			}

			if len(badServers) == 0 {
				info("no cloud servers might be dead right now")
				return
			}
			info("these cloud servers might be dead:")
		}

		for _, server := range badServers {
			if !server.IsBad {
				continue
			}
			var problem string
			if server.Problem != "" {
				problem = "; " + server.Problem
			}
			fmt.Printf(" ID: %s; IP: %s; Name: %s%s\n", server.ID, server.IP, server.Name, problem)
		}

		if cloudServersConfirmDead {
			if len(killedJobs) > 0 {
				info("confirmed that %d running or lost commands on the dead server(s) were lost:", len(killedJobs))
				for _, job := range killedJobs {
					state := string(job.State)
					if state == string(jobqueue.JobStateRunning) {
						var action string
						if job.UntilBuried <= 0 {
							action = "buried"
						} else {
							action = "retried"
						}
						state = fmt.Sprintf("%s - will be %s", state, action)
					}
					fmt.Printf(" Cmd: %s\n  (from server %s, state is now %s)\n", job.Cmd, job.HostID, state)
				}
			} else {
				info("there were no running or lost commands on the dead server(s)")
			}
		}
	},
}

func init() {
	ctx := context.Background()
	RootCmd.AddCommand(cloudCmd)
	cloudCmd.AddCommand(cloudDeployCmd)
	cloudCmd.AddCommand(cloudTearDownCmd)
	cloudCmd.AddCommand(cloudServersCmd)

	// flags specific to these sub-commands
	defaultConfig := internal.DefaultConfig(ctx)
	cloudDeployCmd.Flags().StringVarP(&providerName, "provider", "p", "openstack", "['openstack'] cloud provider")
	cloudDeployCmd.Flags().StringVar(&cloudResourceNameUniquer, "resource_name", realUsername(), fmt.Sprintf("name to be included when naming cloud resources (should be unique to you, max length %d)", maxCloudResourceUsernameLength))
	cloudDeployCmd.Flags().StringVarP(&osPrefix, "os", "o", defaultConfig.CloudOS, "prefix of name, or ID, of the OS image your servers should use")
	cloudDeployCmd.Flags().StringVarP(&osUsername, "username", "u", defaultConfig.CloudUser, "username needed to log in to the OS image specified by --os")
	cloudDeployCmd.Flags().IntVarP(&osRAM, "os_ram", "r", defaultConfig.CloudRAM, "ram (MB) needed by the OS image specified by --os")
	cloudDeployCmd.Flags().IntVarP(&osDisk, "os_disk", "d", defaultConfig.CloudDisk, "minimum disk (GB) for servers")
	cloudDeployCmd.Flags().StringVarP(&flavorRegex, "flavor", "f", defaultConfig.CloudFlavor, "a regular expression to limit server flavors that can be automatically picked")
	defaultNote := ""
	if defaultConfig.CloudFlavorManager == "" {
		defaultNote = " (defaults to --flavor if blank)"
	}
	cloudDeployCmd.Flags().StringVar(&managerFlavor, "manager_flavor", defaultConfig.CloudFlavorManager, "like --flavor, but specific to the first server created to run the manager"+defaultNote)
	cloudDeployCmd.Flags().StringVar(&flavorSets, "flavor_sets", defaultConfig.CloudFlavorSets, "sets of flavors assigned to different hardware, in the form f1,f2;f3,f4")
	cloudDeployCmd.Flags().StringVarP(&postCreationScript, "script", "s", defaultConfig.CloudScript, "path to a start-up script that will be run on each server created")
	cloudDeployCmd.Flags().IntVar(&cloudSpawns, "max_spawns", defaultConfig.CloudSpawns, "maximum number of simultaneous server spawns during scale-up")
	cloudDeployCmd.Flags().IntVar(&maxManagerCores, "max_local_cores", -1, "maximum number of manager cores to use to run cmds; -1 means unlimited")
	cloudDeployCmd.Flags().IntVar(&maxManagerRAM, "max_local_ram", -1, "maximum MB of manager memory to use to run cmds; -1 means unlimited")
	cloudDeployCmd.Flags().StringVarP(&postDeploymentScript, "on_success", "x", defaultConfig.DeploySuccessScript, "path to a script to run locally after a successful deployment")
	cloudDeployCmd.Flags().IntVarP(&serverKeepAlive, "keepalive", "k", defaultConfig.CloudKeepAlive, "how long in seconds to keep idle spawned servers alive for; 0 means forever")
	cloudDeployCmd.Flags().IntVarP(&cloudMaxServers, "max_servers", "m", defaultConfig.CloudServers+1, "maximum number of servers to spawn; 0 means unlimited (default 0)")
	cloudDeployCmd.Flags().StringVar(&cloudGatewayIP, "network_gateway_ip", defaultConfig.CloudGateway, "gateway IP for the created subnet")
	cloudDeployCmd.Flags().StringVar(&cloudCIDR, "network_cidr", defaultConfig.CloudCIDR, "CIDR of the created subnet")
	cloudDeployCmd.Flags().StringVar(&cloudDNS, "network_dns", defaultConfig.CloudDNS, "comma separated DNS name server IPs to use in the created subnet")
	cloudDeployCmd.Flags().StringVarP(&cloudConfigFiles, "config_files", "c", defaultConfig.CloudConfigFiles, "comma separated paths of config files to copy to spawned servers")
	cloudDeployCmd.Flags().IntVarP(&cloudManagerTimeoutSeconds, "timeout", "t", 15, "how long to wait in seconds for the manager to start up")
	cloudDeployCmd.Flags().IntVar(&cloudServersAutoConfirmDead, "auto_confirm_dead", defaultConfig.CloudAutoConfirmDead, "how long to wait in minutes before destroying bad servers; 0 means forever")
	cloudDeployCmd.Flags().BoolVar(&setDomainIP, "set_domain_ip", defaultConfig.ManagerSetDomainIP, "on success, use infoblox to set your domain's IP")
	cloudDeployCmd.Flags().BoolVar(&cloudDebug, "debug", false, "include extra debugging information in the logs, and have runners log to syslog on their machines")

	cloudTearDownCmd.Flags().StringVarP(&providerName, "provider", "p", "openstack", "['openstack'] cloud provider")
	cloudTearDownCmd.Flags().StringVar(&cloudResourceNameUniquer, "resource_name", realUsername(), "name you set during deploy")
	cloudTearDownCmd.Flags().BoolVarP(&forceTearDown, "force", "f", false, "force teardown even when the remote manager cannot be accessed")
	cloudTearDownCmd.Flags().BoolVar(&cloudDebug, "debug", false, "show details of the teardown process")

	cloudServersCmd.Flags().BoolVarP(&cloudServersAll, "all", "a", false, "confirm all maybe dead servers as dead")
	cloudServersCmd.Flags().StringVarP(&cloudServerID, "identifier", "i", "", "identifier of a server to confirm as dead")
	cloudServersCmd.Flags().BoolVarP(&cloudServersConfirmDead, "confirmdead", "d", false, "confirm that 1 (-i) or all (-a) servers are dead [default: just list possibly dead servers]")
	cloudServersCmd.Flags().IntVar(&timeoutint, "timeout", 120, "how long (seconds) to wait to get a reply from 'wr manager'")
}

func bootstrapOnRemote(provider *cloud.Provider, server *cloud.Server, exe string, mp int, wp int, keyPath string, wrMayHaveStarted bool, domainMatchesIP bool) {
	ctx := context.Background()

	// upload ourselves to /tmp
	remoteExe := filepath.Join(cloudBinDir, "wr")
	err := server.UploadFile(ctx, exe, remoteExe)
	if err != nil && !wrMayHaveStarted {
		teardown(ctx, provider)
		die("failed to upload wr to the server at %s: %s", server.IP, err)
	}

	// create a config file on the remote to have the remote wr work on the same
	// ports that we'd use locally, use the right domain, and have it use an S3
	// db backup location if configured
	dbBk := "db_bk"
	if internal.IsRemote(config.ManagerDBBkFile) {
		dbBk = config.ManagerDBBkFile
	} else if config.IsProduction() {
		// copy over our database
		if _, errf := os.Stat(config.ManagerDBFile); errf == nil {
			if errf = server.UploadFile(ctx, config.ManagerDBFile,
				filepath.Join("./.wr_"+config.Deployment, "db")); errf == nil {
				info("copied local database to remote server")
			} else if !wrMayHaveStarted {
				teardown(ctx, provider)
				die("failed to upload local database to the server at %s: %s", server.IP, errf)
			}
		} else if !os.IsNotExist(errf) {
			teardown(ctx, provider)
			die("failed to access the local database: %s", errf)
		}
	}
	if err = server.CreateFile(ctx, fmt.Sprintf("managerport: \"%d\"\nmanagerweb: \"%d\"\nmanagerdbbkfile: \"%s\"\nmanagercertdomain: \"%s\"\nmanagerumask: %d\n", mp, wp, dbBk, config.ManagerCertDomain, config.ManagerUmask), wrConfigFileName); err != nil {
		teardown(ctx, provider)
		die("failed to create our config file on the server at %s: %s", server.IP, err)
	}

	// copy over our token file, if we're in a recovery situation
	if _, errf := os.Stat(config.ManagerTokenFile); errf == nil {
		if errf = server.UploadFile(ctx, config.ManagerTokenFile, filepath.Join("./.wr_"+config.Deployment, "client.token")); errf == nil {
			info("copied existing client.token to remote server")
		}
	}

	if _, _, err = server.RunCmd(ctx, "chmod u+x "+remoteExe, false); err != nil && !wrMayHaveStarted {
		teardown(ctx, provider)
		die("failed to make remote wr executable: %s", err)
	}

	// copy over our cloud resource details, including our ssh key
	cRN := cloudResourceName(cloudResourceNameUniquer)
	localResourceFile := filepath.Join(config.ManagerDir, "cloud_resources."+providerName+"."+cRN)
	remoteResourceFile := filepath.Join("./.wr_"+config.Deployment, "cloud_resources."+providerName+"."+cRN)
	if err = server.UploadFile(ctx, localResourceFile, remoteResourceFile); err != nil && !wrMayHaveStarted {
		teardown(ctx, provider)
		die("failed to upload wr cloud resources file to the server at %s: %s", server.IP, err)
	}
	localKeyFile := filepath.Join(config.ManagerDir, "cloud_resources."+providerName+".key")

	if err = os.WriteFile(localKeyFile, []byte(provider.PrivateKey()), 0600); err != nil {
		teardown(ctx, provider)
		die("failed to create key file %s: %s", localKeyFile, err)
	}
	remoteKeyFile := filepath.Join("./.wr_"+config.Deployment, "cloud_resources."+providerName+".key")
	if err = server.UploadFile(ctx, localKeyFile, remoteKeyFile); err != nil && !wrMayHaveStarted {
		teardown(ctx, provider)
		die("failed to upload wr cloud key file to the server at %s: %s", server.IP, err)
	}
	_, _, err = server.RunCmd(ctx, "chmod 600 "+remoteResourceFile, false)
	if err != nil {
		warn("failed to chmod 600 %s: %s", remoteResourceFile, err)
	}
	_, _, err = server.RunCmd(ctx, "chmod 600 "+remoteKeyFile, false)
	if err != nil {
		warn("failed to chmod 600 %s: %s", remoteKeyFile, err)
	}

	// copy over our ca, cert and key files
	remoteCertFile := filepath.Join("./.wr_"+config.Deployment, "cert.pem")
	if err = server.UploadFile(ctx, config.ManagerCertFile, remoteCertFile); err != nil && !wrMayHaveStarted {
		teardown(ctx, provider)
		die("failed to upload wr manager certificate file to the server at %s: %s", server.IP, err)
	}
	remoteKeyFile = filepath.Join("./.wr_"+config.Deployment, "key.pem")
	if err = server.UploadFile(ctx, config.ManagerKeyFile, remoteKeyFile); err != nil && !wrMayHaveStarted {
		teardown(ctx, provider)
		die("failed to upload wr manager key file to the server at %s: %s", server.IP, err)
	}
	_, _, err = server.RunCmd(ctx, "chmod 600 "+remoteCertFile, false)
	if err != nil {
		warn("failed to chmod 600 %s: %s", remoteCertFile, err)
	}
	_, _, err = server.RunCmd(ctx, "chmod 600 "+remoteKeyFile, false)
	if err != nil {
		warn("failed to chmod 600 %s: %s", remoteKeyFile, err)
	}
	_, err = os.Stat(config.ManagerCAFile)
	if err == nil {
		remoteCAFile := filepath.Join("./.wr_"+config.Deployment, "ca.pem")
		if err = server.UploadFile(ctx, config.ManagerCAFile, remoteCAFile); err != nil && !wrMayHaveStarted {
			teardown(ctx, provider)
			die("failed to upload wr manager CA file to the server at %s: %s", server.IP, err)
		}
		_, _, err = server.RunCmd(ctx, "chmod 600 "+remoteCAFile, false)
		if err != nil {
			warn("failed to chmod 600 %s: %s", remoteCAFile, err)
		}
	}

	// start up the manager
	var alreadyStarted bool
	if wrMayHaveStarted {
		response, _, errf := server.RunCmd(ctx, fmt.Sprintf("%s manager status --deployment %s", remoteExe, config.Deployment), false)
		if errf == nil && response == "started\n" {
			alreadyStarted = true
		}
	}
	if !alreadyStarted {
		// create a file containing all the env vars for this provider, so that
		// we can source it later
		envvars, erra := cloud.AllEnv(providerName)
		if erra != nil {
			die("failed to get needed environment variables: %s", erra)
		}
		envvarExports := ""
		for _, env := range envvars {
			val := os.Getenv(env)
			if val == "" {
				continue
			}
			// *** this is bash-like only; is that a problem?
			envvarExports += fmt.Sprintf("export %s=\"%s\"\n", env, val)
		}
		err = server.CreateFile(ctx, envvarExports, wrEnvFileName)
		if err != nil {
			teardown(ctx, provider)
			die("failed to create our environment variables file on the server at %s: %s", server.IP, err)
		}
		_, _, err = server.RunCmd(ctx, "chmod 600 "+wrEnvFileName, false)
		if err != nil {
			warn("failed to chmod 600 %s: %s", wrEnvFileName, err)
		}

		var postCreationArg string
		if postCreationScript != "" {
			// copy over the post creation script to the server so remote
			// manager can use it
			remoteScriptFile := filepath.Join("./.wr_"+config.Deployment, "cloud_resources."+providerName+".script")
			err = server.UploadFile(ctx, postCreationScript, remoteScriptFile)
			if err != nil && !wrMayHaveStarted {
				teardown(ctx, provider)
				die("failed to upload wr cloud script file to the server at %s: %s", server.IP, err)
			}

			postCreationArg = " -p " + remoteScriptFile
		}

		var configFilesArg string
		if cloudConfigFiles != "" {
			// strip any local file locations
			var remoteConfigFiles []string
			for _, cf := range strings.Split(cloudConfigFiles, ",") {
				parts := strings.Split(cf, ":")
				if len(parts) == 2 {
					remoteConfigFiles = append(remoteConfigFiles, parts[1])
				} else {
					remoteConfigFiles = append(remoteConfigFiles, cf)
				}
			}

			configFilesArg = " --cloud_config_files '" + strings.Join(remoteConfigFiles, ",") + "'"
		}

		var flavorArg string
		if flavorRegex != "" {
			flavorArg = " -l '" + flavorRegex + "'"
		}
		if flavorSets != "" {
			flavorArg += " --cloud_flavor_sets '" + flavorSets + "'"
		}

		var osDiskArg string
		if osDisk > 0 {
			osDiskArg = " -d " + strconv.Itoa(osDisk)
		}

		// get the manager running
		m := cloudMaxServers - 1
		debugStr := ""
		if cloudDebug {
			debugStr = " --debug --runner_debug"
		}
		useCertDomainStr := ""
		if domainMatchesIP {
			useCertDomainStr = " --use_cert_domain"
		}
		mCmd := fmt.Sprintf("source %s && %s manager start --deployment %s -s %s -k %d -o '%s' -r %d -m %d -u %s%s%s%s%s  --cloud_cidr '%s' --local_username '%s' --cloud_spawns %d --max_cores %d --max_ram %d --timeout %d --cloud_auto_confirm_dead %d%s%s && rm %s", wrEnvFileName, remoteExe, config.Deployment, providerName, serverKeepAlive, osPrefix, osRAM, m, osUsername, postCreationArg, flavorArg, osDiskArg, configFilesArg, cloudCIDR, cloudResourceNameUniquer, cloudSpawns, maxManagerCores, maxManagerRAM, cloudManagerTimeoutSeconds, cloudServersAutoConfirmDead, useCertDomainStr, debugStr, wrEnvFileName)

		var e string
		_, e, err = server.RunCmd(ctx, mCmd, false)
		if err != nil {
			warn("failed to start wr manager on the remote server")
			if len(e) > 0 {
				color.Red(e)
			}

			// copy over any manager logs that got created locally (ignore
			// errors, and overwrite any existing file)
			cloudLogFilePath := config.ManagerLogFile + "." + providerName
			errf := server.DownloadFile(ctx, filepath.Join("./.wr_"+config.Deployment, "log"), cloudLogFilePath)

			// display any non-info lines in that log file
			if errf == nil {
				f, errf := os.Open(cloudLogFilePath)
				if errf == nil {
					explained := false
					scanner := bufio.NewScanner(f)
					for scanner.Scan() {
						line := scanner.Text()
						if !strings.Contains(line, "lvl=info") {
							if !explained {
								fmt.Println("")
								warn("remote manager logs:")
								explained = true
							}
							color.Red(line)
						}
					}
					if explained {
						fmt.Println("")
					}
				}
			}

			warn("To debug further you can try to ssh to this server using:")
			color.Magenta("ssh -i %s %s@%s", keyPath, osUsername, server.IP)
			fmt.Printf("and see if you can run (checking %s afterwards):\n", "~/.wr_"+config.Deployment+"/log")
			color.Magenta(mCmd)

			// now teardown and die, once the user confirms
			warn("Once you're done debugging, hit return to teardown")
			var response string
			_, errs := fmt.Scanln(&response)
			if errs != nil && !strings.Contains(errs.Error(), "unexpected newline") {
				warn("failed to read your response: %s", errs)
			}

			teardown(ctx, provider)
			die("toredown following failure to start the manager remotely")
		}

		// wait a few seconds for the manager to start listening on its ports
		<-time.After(3 * time.Second)
	}

	remoteTokenFile := filepath.Join("./.wr_"+config.Deployment, "client.token")
	err = server.DownloadFile(ctx, remoteTokenFile, config.ManagerTokenFile)
	if err != nil {
		teardown(ctx, provider)
		die("could not make a local copy of the authentication token: %s", err)
	}
}

func startForwarding(serverIP, serverUser, keyFile string, port int, pidPath string) error {
	// first check if pidPath already has a pid and if that pid is alive
	if _, running := checkProcess(pidPath); running {
		//info("assuming the process with id %d is already forwarding port %d to %s:%d", pid, port, serverIP, port)
		return nil
	}

	// start ssh -L running
	cmd := exec.Command("/usr/bin/ssh", "-i", keyFile, "-o", "ExitOnForwardFailure yes", "-o", "UserKnownHostsFile /dev/null", "-o", "StrictHostKeyChecking no", "-qngNTL", fmt.Sprintf("%d:0.0.0.0:%d", port, port), fmt.Sprintf("%s@%s", serverUser, serverIP)) // #nosec
	err := cmd.Start()
	if err != nil {
		return err
	}

	// store ssh's pid to file
	err = os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0600)

	// don't cmd.Wait(); ssh will continue running in the background after we
	// exit

	return err
}

func checkProcess(pidPath string) (pid int, running bool) {
	// read file (treat errors such as file not existing as no process)
	pidBytes, err := os.ReadFile(pidPath)
	if err != nil {
		return pid, running
	}

	// convert file contents to pid (also treating errors as no process)
	pid, err = strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		return pid, running
	}

	// see if the pid is running
	process, err := os.FindProcess(pid)
	if err != nil {
		return pid, running
	}
	err = process.Signal(syscall.Signal(0))
	running = err == nil
	return pid, running
}

func killProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return process.Signal(syscall.Signal(9))
}

func teardown(ctx context.Context, p *cloud.Provider) {
	if err := p.TearDown(ctx); err != nil {
		warn("teardown failed: %s", err)
	}
}
