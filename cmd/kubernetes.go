// Copyright © 2018, 2021 Genome Research Limited
// Author: Theo Barber-Ban <tb15@sanger.ac.uk>.
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
	"encoding/gob"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/kubernetes/client"
	kubedeployment "github.com/VertebrateResequencing/wr/kubernetes/deployment"
	"github.com/kardianos/osext"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wr/clog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// podBinDir is where we will upload executables to our created pod. It is a
// volume mount added to the init container and the container that will run wr.
// As defining a volume mount overwrites whatever is in that directory we want
// this to be unique. This is also what $HOME is set to, allowing paths of the
// form '~/' to still work. Anything not copied into podBinDir will be lost when
// the init container completes. This is why all files to be copied over are
// rewritten into the form ~/foo/bar (Or in special cases, hard coded to
// podBinDir)
const podBinDir = "/wr-tmp/"

// podScriptDir is where the configMap will be mounted.
const podScriptDir = "/scripts/"

// linuxBinaryName, the name of the wr linux binary to be expected. This is
// passed to the config map that is set as the entry point for the chosen
// container. This way we can ensure the users post creation script starts
// before the main command.
const linuxBinaryName = "/wr"

// kubeLogFileName is the name of the file to write the deployment logs to.
const kubeLogFileName = "k8sDeployLog"

// options for this cmd
var podPostCreationScript string
var containerImage string
var podConfigFiles string
var kubeDebug bool
var kubeNamespace string
var maxPods int
var configMapName string
var kubeConfig string

// kubeCmd represents the cloud command
var kubeCmd = &cobra.Command{
	Use:   "k8s",
	Short: "Kubernetes cluster interfacing",
	Long: `Kubernetes cluster interfacing.

To run wr on a kubernetes cluster, you need to deploy the "wr manager" to a
unique namespace. From there the manager will run your commands on additional
pods spawned as demand dictates.

The k8s sub-commands make it easy to get started, interact with that remote
manager, and clean up afterwards.`,
}

// deploy sub-command brings up a "head" pod in the cluster and starts a proxy
// daemon to interact with the manager we spawn there
var kubeDeployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy a manager to a kubernetes cluster",
	Long: `Start up 'wr manager' on a kubernetes cluster.

Deploy creates a 'wr manager' pod. In a production deployment the remote manager
will use a copy of the latest version of the wr database, taken from your S3 db
backup location, or if you don't use S3, from your local filesystem.

Deploy then sets up port forwarding in the background that lets you use the
normal wr command line utilities such as 'wr add' and view the wr website
locally, even though the manager is actually running remotely. Note that this
precludes starting wr manager locally as well. Also be aware that while 'wr add'
normally associates your current environment variables and working directory
with the cmds you want to run, with a remote deployment the working directory
defaults to /tmp, and commands will be run with the non-login environment
variables of the pod the command is run on.

The --script option value can be, for example, the path to a bash script that
you want to run on any created pod before any commands run on them. You might
install some software for example. Note that the script is run by default as
root. If your bash script has commands with 'sudo' you may need to install sudo.
This is usually when the image does not include it (e.g the ubuntu images). For
debian based images this may look like 'apt-get -y install sudo'.

The --config_files option lets you specify comma separated arbitrary text file
paths that should be copied from your local system to any created pods.
Currently due to limitations in the way files are copied to pods, only files
with a destination "~/foo/bar" will be copied. For files that should be
transferred from your home directory to the pod's home directory (which could be
at different absolute paths), prefix your path with "~/". If the local path of a
file is unrelated to the remote path, separate the paths with a colon to specify
source and destination, eg. "~/projectSpecific/.s3cfg:~/.s3cfg". Local paths
that don't exist are silently ignored. This option is important if you want to
be able to queue up commands that rely on the --mounts option to 'wr add': you'd
specify your s3 config file(s) which contain your credentials for connecting to
your s3 bucket(s).

Deploy can work with most container images because it uploads wr to any pod it
creates; your image does not have to have wr installed on it. The only
requirements of the image are that it has tar, cat and bash installed. (Please
only use bash for your shell.)

For --mounts to work, fuse-utils must be installed, and /etc/fuse.conf should
already have user_allow_other set. If using a minimal ubuntu image (such as the
default), you might therefore supply a --script file that contains:
apt-get update && apt-get install -y ca-certificates fuse
(If all your jobs will use mounts, it makes more sense to specify a
--container_image that has fuse pre-installed.)

By default the 'ubuntu:latest' image is used. Currently any container registry
natively supported by kubernetes should work, though there is no support for
secrets so some private registries may not work (Node authentication should).

See https://kubernetes.io/docs/concepts/containers/images/ for more details.

Authenticating against the cluster will be attempted, by default, with the file
pointed to by the $KUBECONFIG variable, else ~/.kube/config.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		// Read in post creation script
		var postCreation []byte
		var extraArgs []string
		if podPostCreationScript != "" {
			var err error
			postCreation, err = os.ReadFile(podPostCreationScript)
			if err != nil {
				die("--script %s could not be read: %s", podPostCreationScript, err)
			}
			// daemon runs from /, so we need to convert relative to absolute
			// path *** and then pretty hackily, re-specify the option by
			// repeating it on the end of os.Args, where the daemonization code
			// will pick it up
			pcsAbs, err := filepath.Abs(podPostCreationScript)
			if err != nil {
				die("--script %s could not be converted to an absolute path: %s", podPostCreationScript, err)
			}
			if pcsAbs != postCreationScript {
				extraArgs = append(extraArgs, "--script")
				extraArgs = append(extraArgs, pcsAbs)
			}
		} else {
			podPostCreationScript = "nil.sh"
		}

		// first we need our working directory to exist
		createWorkingDir()

		// check to see if the manager is already running (regardless of the
		// state of the pid file); we can't proxy if a manager is already up
		jq := connect(1*time.Second, true)
		if jq != nil {
			die("wr manager on port %s is already running (pid %d); please stop it before trying again.", config.ManagerPort, jq.ServerInfo.PID)
		}

		// now check if there's a daemon running. If it is the forwarding
		// must've died. (Closed laptop?) If so kill the now useless daemon.
		// This avoids the 'resource unavaliable' daemonising error.
		fmPidFile := filepath.Join(config.ManagerDir, "kubernetes_resources.fw.pid")
		fmPid, fmRunning := checkProcess(fmPidFile)

		if fmRunning {
			info("killing stale daemon with PID %d", fmPid)
			stale, err := os.FindProcess(fmPid)
			if err != nil {
				warn("Failed to find process: %s", err)
			}
			errr := stale.Kill()
			if errr != nil {
				warn("Killing process returned error: %s", errr)
			}
		}

		// *** can't figure out how to get non-localhost certs to work
		if config.ManagerCertDomain != "" && config.ManagerCertDomain != "localhost" {
			die("non-localhost domains are not currently supported with kubernetes deployments; reconfigure wr to use its own self-generated SSL certficate for localhost")
		}

		// later we will copy our server cert and key to the manager pod; if we
		// don't have any, generate them now
		err := internal.CheckCerts(config.ManagerCertFile, config.ManagerKeyFile)
		if err != nil {
			err = internal.GenerateCerts(config.ManagerCAFile, config.ManagerCertFile, config.ManagerKeyFile, config.ManagerCertDomain)
			if err != nil {
				die("could not generate certs: %s", err)
			}
			info("created a new key and certificate for TLS")
		}

		// we will spawn wr on the remote server we will create, which means we
		// need to know the path to ourselves in case we're not in the user's
		// $PATH
		exe, err := osext.Executable()
		if err != nil {
			die("could not get the path to wr: %s", err)
		}

		// we then  need to rewrite it to always use the 'wr-linux' binary, in
		// case we are deploying from a mac.
		exe = filepath.Dir(exe) + linuxBinaryName

		// get all necessary cloud resources in place
		mp, err := strconv.Atoi(config.ManagerPort)
		if err != nil {
			die("bad manager_port [%s]: %s", config.ManagerPort, err)
		}

		wp, err := strconv.Atoi(config.ManagerWeb)
		if err != nil {
			die("bad manager_web [%s]: %s", config.ManagerWeb, err)
		}

		// Set up the client and resource files
		c := kubedeployment.Controller{
			Client: &client.Kubernetesp{},
		}
		resourcePath := filepath.Join(config.ManagerDir, "kubernetes_resources")

		// Authenticate and populate Kubernetesp with clientset and restconfig.
		c.Clientset, c.Restconfig, err = c.Client.Authenticate(ctx, client.AuthConfig{KubeConfigPath: kubeConfig})
		if err != nil {
			die("could not get authentication details for the cluster: %s", err)
		}

		_, err = c.Clientset.CoreV1().Namespaces().List(metav1.ListOptions{})
		if err != nil {
			die("could not connect to the cluster: %s", err)
		}

		// Check if an existing deployment with the label 'app=wr-manager'
		// exists. Read in namespace from resource file, if no file exists and
		// no namespace is passed as a flag create new namespace and redeploy.
		// If a resource file exists or a namespace is passed as a flag check to
		// see if there is an existing manager deployment to reconnect to

		var kubeDeploy bool //defaults false

		// A namespace is passed, it takes priority.
		if len(kubeNamespace) != 0 {
			// Check if a previous deployment exists
			exists, err := c.Client.CheckWRDeploymentExists(kubeNamespace)
			if err != nil {
				die("wr-manager deployment found but an error occurred: %s", err)
			}
			// If no previous deployment exists, create one. If a deployment exists, check its health.
			if !exists {
				kubeDeploy = true
			} else {
				healthy, err := c.Client.CheckWRDeploymentHealthy(kubeNamespace)
				if !healthy {
					die("wr-manager deployment found but not healthy: %s", err)
				}
			}
		} else {
			// Look for a set of resources in the manager directory If found,
			// load them, otherwise use a new empty set.
			if _, serr := os.Stat(resourcePath); os.IsNotExist(serr) {
				kubeDeploy = true
			} else {
				// Read the namespace resource file
				resources, err := openResources(ctx, resourcePath)
				if err != nil {
					die("failed to open resource file with path %s: %s", resourcePath, err)
				}

				namespace := resources.Details["namespace"]

				// Check if a previous deployment exists
				exists, err := c.Client.CheckWRDeploymentExists(namespace)
				if err != nil {
					die("wr-manager deployment found but an error occurred: %s", err)
				}
				// If no previous deployment exists, create one. If a deployment exists, check its health.
				if !exists {
					kubeDeploy = true
				} else {
					healthy, err := c.Client.CheckWRDeploymentHealthy(namespace)
					if !healthy {
						die("wr-manager deployment found but not healthy: %s", err)
					}
				}
			}
		}

		if !kubeDeploy {
			info("found existing healthy wr-manager deployment, reconnecting")
		} else {
			info("please wait while wr is deployed to the cluster")
		}

		// later we will copy a file that configures wr with the correct ports
		// and db backup; create that as a temp file now
		tmpConfigFile, errt := os.CreateTemp("", ".wr_k8s_config")
		if errt != nil {
			die("cannot create temporary config file: %s", errt)
		}
		defer func() {
			errr := os.Remove(tmpConfigFile.Name())
			if errr != nil {
				warn("failed to remove temporary config file: %s", errr)
			}
		}()
		_, errt = tmpConfigFile.Write([]byte(fmt.Sprintf("managerport: \"%d\"\nmanagerweb: \"%d\"\nmanagerdbbkfile: \"%s\"\n",
			mp, wp, config.ManagerDBBkFile)))
		if errt != nil {
			die("failed to write to temporary config file: %s", errt)
		}
		errc := tmpConfigFile.Close()
		if errc != nil {
			die("failed to close temporary config file: %s", errc)
		}

		// Daemonise
		fwPidPath := filepath.Join(config.ManagerDir, "kubernetes_resources.fw.pid")
		umask := 007
		child, context := daemonize(fwPidPath, umask, extraArgs...)
		if child != nil {
			// PostParent() (Runs in the parent process after spawning child)
			jq = connect(120*time.Second, true)
			if jq == nil {
				die("could not talk to wr manager after 120s")
			}

			// The remote manager is running, read the resource file to
			// determine the name of the pod to fetch the client.token from.

			// Read the manager pod's name from resource file
			resources, err := openResources(ctx, resourcePath)
			if err != nil {
				die("failed to open resource file with path %s: %s", resourcePath, err)
			}
			managerPodName := resources.Details["manager-pod"]
			namespace := resources.Details["namespace"]

			// cat the contents of the client.token in the running manager, so
			// we can write them to disk locally, and provide the URL for
			// accessing the web interface
			stdOut, _, err := c.Client.ExecInPod(managerPodName, "wr-manager", namespace, []string{"cat", podBinDir + ".wr_" + config.Deployment + "/client.token"})
			if err != nil {
				die("something went wrong executing the command to retrieve the token: %s", err)
			}
			token := stdOut

			// Write token to file
			err = os.WriteFile(config.ManagerTokenFile, []byte(token), 0644)
			if err != nil {
				warn("Failed to write token to file: %s", err)
			}
			info("wr manager remotely started in namespace, pod: %s %s (%s)", namespace, managerPodName, sAddr(jq.ServerInfo))
			info("wr's web interface can be reached locally at https://%s:%s/?token=%s", jq.ServerInfo.Host, jq.ServerInfo.WebPort, token)
		} else {
			// daemonized child, that will run until signalled to stop

			// Set up logging to file
			kubeLogFile := filepath.Join(config.ManagerDir, kubeLogFileName)
			if err := clog.ToFileAtLevel(kubeLogFile, "warn"); err != nil {
				warn("wr manager could not log to %s: %s", kubeLogFile, err)
			}

			defer func() {
				errr := context.Release()
				if errr != nil {
					warn("daemon release failed: %s", errr)
				}
			}()

			debugStr := ""
			if cloudDebug {
				debugStr = " --debug"
			}

			// we will have 2 configMaps, one for the manager pod that is
			// hard-coded to be ubuntu where we will install certs and fuse,
			// and one as the default for spawned runner pods
			var managerConfigMapName string

			// Look for a set of resources in the manager directory If found,
			// load them else use a new empty set.
			if _, serr := os.Stat(resourcePath); os.IsNotExist(serr) {
				info("Using new set of resources, none found.")
				resources := &cloud.Resources{
					ResourceName: "Kubernetes",
					Details:      make(map[string]string),
					PrivateKey:   "",
					Servers:      make(map[string]*cloud.Server)}

				// Populate the rest of Kubernetesp. If there is a predefined
				// namespace set, use it.
				if len(kubeNamespace) != 0 {
					err = c.Client.Initialize(ctx, c.Clientset, kubeNamespace)
				} else {
					err = c.Client.Initialize(ctx, c.Clientset)
				}
				if err != nil {
					die("Failed to initialise clients: %s", err)
				}

				// Create the configMap
				pc := string(postCreation)
				cmap, errc := c.Client.CreateInitScriptConfigMap(pc)
				if errc != nil {
					die("Failed to create config map: %s", errc)
				}
				configMapName = cmap.ObjectMeta.Name

				pc = "apt-get update\napt-get install -y ca-certificates fuse\n"
				cmap, errc = c.Client.CreateInitScriptConfigMap(pc)
				if errc != nil {
					die("Failed to create config map: %s", errc)
				}
				managerConfigMapName = cmap.ObjectMeta.Name

				kubeNamespace = c.Client.NewNamespaceName

				// Store the namespace and configMapName for fun and profit.
				resources.Details["namespace"] = kubeNamespace
				resources.Details["configMapName"] = configMapName
				resources.Details["managerConfigMapName"] = managerConfigMapName

				// Save resources.
				file, erro := os.OpenFile(resourcePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
				if erro != nil {
					warn("failed to open resource file %s for writing: %s", resourcePath, erro)
				}
				encoder := gob.NewEncoder(file)
				err = encoder.Encode(resources)
				if err != nil {
					warn("Failed to encode resource file: %s", err)
				}
				internal.LogClose(ctx, file, "resource file", "path", resourcePath)
			} else {
				resources, erro := openResources(ctx, resourcePath)
				if erro != nil {
					die("failed to open resource file with path %s: %s", resourcePath, erro)
				}
				kubeNamespace = resources.Details["namespace"]
				configMapName = resources.Details["configMapName"]
				managerConfigMapName = resources.Details["managerConfigMapName"]

				// Populate the rest of Kubernetesp
				info("initialising to namespace %s", kubeNamespace)
				err = c.Client.Initialize(ctx, c.Clientset, kubeNamespace)
				if err != nil {
					die("Failed to initialise client to namespace %s: %s", kubeNamespace, err)
				}
			}

			remoteExe := filepath.Join(podBinDir, linuxBinaryName)
			m := maxPods - 1

			mCmd := fmt.Sprintf("%s manager start -f --deployment %s --scheduler kubernetes --namespace %s --cloud_keepalive %d  --cloud_servers %d --config_map %s --cloud_os %s --cloud_config_files '%s' --timeout %d%s --local_username %s",
				remoteExe, config.Deployment, kubeNamespace, serverKeepAlive, m, configMapName, containerImage, podConfigFiles, managerTimeoutSeconds, debugStr, realUsername())

			mCmd = strings.Replace(mCmd, "'", "", -1)
			if kubeDebug {
				mCmd = mCmd + " --debug"
			}

			binaryArgs := []string{mCmd}

			// Add the configFiles passed to the deploy cmd
			files := rewriteConfigFiles(podConfigFiles)
			// Copy the wr-linux binary
			files = append(files, client.FilePair{Src: exe, Dest: podBinDir})
			// Copy cert, key & ca files
			files = append(files, client.FilePair{Src: config.ManagerKeyFile, Dest: podBinDir + ".wr_" + config.Deployment + "/key.pem"})
			files = append(files, client.FilePair{Src: config.ManagerCAFile, Dest: podBinDir + ".wr_" + config.Deployment + "/ca.pem"})
			files = append(files, client.FilePair{Src: config.ManagerCertFile, Dest: podBinDir + ".wr_" + config.Deployment + "/cert.pem"})
			// Copy the temp config file for configuring wr
			files = append(files, client.FilePair{Src: tmpConfigFile.Name(), Dest: podBinDir + ".wr_config.yml"})

			// Specify deployment options
			c.Opts = &kubedeployment.DeployOpts{
				ContainerImage:  containerImage,
				TempMountPath:   podBinDir,
				Files:           files,
				BinaryPath:      podScriptDir + client.DefaultScriptName,
				BinaryArgs:      binaryArgs,
				ConfigMapName:   configMapName,
				ConfigMountPath: podScriptDir,
				RequiredPorts:   []int{mp, wp},
				ResourcePath:    resourcePath,
			}

			// Create the deployment if an existing one does not exist
			if kubeDeploy {
				info("creating wr deployment")
				err = c.Client.Deploy(ctx, c.Opts.TempMountPath, c.Opts.BinaryPath, c.Opts.BinaryArgs, managerConfigMapName,
					c.Opts.ConfigMountPath, c.Opts.RequiredPorts)
				if err != nil {
					die("failed to create deployment: %s", err)
				}
			}

			// Start Controller running, stopping it on signal
			sigs := make(chan os.Signal, 2)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
			stopCh := make(chan struct{})
			info("starting controller")
			go c.Run(ctx, stopCh)
			<-sigs
			close(stopCh)
			info("gracefully stopped controller after receiving signal")
		}
	},
}

// teardown sub-command deletes all kubernetes resources we created and then
// stops the daemon by killing it's pid.
var kubeTearDownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Delete all kubernetes resources that deploy created",
	Long: `Immediately stop the remote workflow manager, saving its state.

Deletes all kubernetes resources that wr created (pods, deployments, config maps, namespaces).
(Except for any files that were saved to persistent cloud storage.)

Note that any runners that are currently running will die, along with any
commands they were running. It is more graceful to issue 'wr manager drain'
first, and regularly rerun drain until it reports the manager is stopped, and
only then request a teardown (you'll need to add the --force option). But this
is only a good idea if you have configured wr to back up its database to S3, as
otherwise your database going forward will not reflect anything you did during
that kubernetes deployment.

If you don't back up to S3, the teardown command tries to copy the remote
database locally, which is only possible while the pod is still up and
accessible.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		setupLogging(kubeDebug)

		// before stopping the manager, make sure we can interact with the
		// cluster - that our credentials are correct.
		Client := &client.Kubernetesp{}
		clientset, _, err := Client.Authenticate(ctx, client.AuthConfig{KubeConfigPath: kubeConfig})
		if err != nil {
			die("could not get authentication details for the cluster: %s", err)
		}
		_, err = clientset.CoreV1().Namespaces().List(metav1.ListOptions{})
		if err != nil {
			die("could not connect to the cluster: %s", err)
		}

		resourcePath := filepath.Join(config.ManagerDir, "kubernetes_resources")

		resources, err := openResources(ctx, resourcePath)
		if err != nil {
			die("failed to open resource file with path %s: %s", resourcePath, err)
		}

		// now check if the ssh forwarding is up
		fmPidFile := filepath.Join(config.ManagerDir, "kubernetes_resources.fw.pid")
		fmPid, fmRunning := checkProcess(fmPidFile)

		// try and stop the remote manager
		noManagerMsg := "; deploy first or use --force option"
		noManagerForcedMsg := "; tearing down anyway - you may lose changes if not backing up the database to S3!"
		serverHadProblems := false
		if fmRunning {
			jq := connect(1*time.Second, true)
			if jq != nil {
				var syncMsg string
				if internal.IsRemote(config.ManagerDBBkFile) {
					if _, errf := os.Stat(config.ManagerDBFile); !os.IsNotExist(errf) {
						// Move aside the local database so that if the manager
						// is started locally, the database will be restored
						// from S3 and have the history of what was run in the
						// cloud.
						if errf = os.Rename(config.ManagerDBFile, config.ManagerDBFile+".old"); err == nil {
							syncMsg = "; the local database will be updated from S3 if manager started locally"
						} else {
							warn("could not rename the local database; if the manager is started locally, it will not be updated with the latest changes in S3! %s", errf)
						}
					}
				} else {
					// Copy the remote database locally, so if the manager is
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

		// cat logfiles and write to disk.
		kubeSchedulerLog, _, err := Client.ExecInPod(resources.Details["manager-pod"], "wr-manager", resources.Details["namespace"], []string{"cat", podBinDir + ".wr_" + config.Deployment + "/kubeSchedulerLog"})
		if err != nil {
			warn("error retrieving kubeSchedulerLog file: %s", err)
		}
		kubeSchedulerControllerLog, _, err := Client.ExecInPod(resources.Details["manager-pod"], "wr-manager", resources.Details["namespace"], []string{"cat", podBinDir + ".wr_" + config.Deployment + "/kubeSchedulerControllerLog"})
		if err != nil {
			warn("error retrieving kubeSchedulerControllerLog file: %s", err)
		}

		// write logs to file
		err = os.WriteFile(config.ManagerDir+"/kubeSchedulerLog", []byte(kubeSchedulerLog), 0644)
		if err != nil {
			warn("failed to write kubeSchedulerLog to file: %s", err)
		}
		err = os.WriteFile(config.ManagerDir+"/kubeSchedulerControllerLog", []byte(kubeSchedulerControllerLog), 0644)
		if err != nil {
			warn("failed to write kubeSchedulerControllerLog to file: %s", err)
		}

		// get the unified log last, so we can scan it.
		log, _, errl := Client.ExecInPod(resources.Details["manager-pod"], "wr-manager", resources.Details["namespace"], []string{"cat", podBinDir + ".wr_" + config.Deployment + "/log"})
		if errl != nil {
			warn("error retrieving log file: %s", errl)
		}
		errf := os.WriteFile(config.ManagerDir+"/log", []byte(log), 0644)
		if errf != nil {
			warn("failed to write log to file: %s", errf)
		}

		// scan the log file for critical errors
		if errf == nil && errl == nil {
			f, errfo := os.Open(config.ManagerDir + "/log")
			if errfo == nil {
				explained := false
				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					line := scanner.Text()
					if strings.Contains(line, "lvl=crit") {
						if !explained {
							warn("looks like the manager in the pod suffered critical errors:")
							explained = true
						}
						fmt.Println(line)
					}
				}
				if serverHadProblems {
					info("the remote manager log has been saved to %s", config.ManagerDir+"/log")
				}
			}
		}

		// teardown kubernetes resources we created
		nameSpace := kubeNamespace
		if nameSpace == "" {
			nameSpace = resources.Details["namespace"]
		}
		info("deleting namespace %s", nameSpace)
		err = Client.TearDown(ctx, nameSpace)
		if err != nil {
			die("failed to delete the kubernetes resources previously created: %s", err)
		}

		err = os.Remove(filepath.Join(config.ManagerDir, "kubernetes_resources"))
		if err != nil && !os.IsNotExist(err) {
			warn("failed to delete the kubernetes resources file: %s", err)
		}
		err = os.Remove(filepath.Join(config.ManagerDir + "/client.token"))
		if err != nil && !os.IsNotExist(err) {
			warn("failed to delete the client token : %s", err)
		}

		info("deleted all kubernetes resources previously created")

		// kill the port forwarders
		if fmRunning {
			err = killProcess(fmPid)
			if err == nil {
				err = os.Remove(fmPidFile)
				if err != nil && !os.IsNotExist(err) {
					warn("failed to remove the forwarder pid file %s: %s", fmPidFile, err)
				}
			}
		}
	},
}

func init() {
	ctx := context.Background()
	RootCmd.AddCommand(kubeCmd)
	kubeCmd.AddCommand(kubeDeployCmd)
	kubeCmd.AddCommand(kubeTearDownCmd)

	// flags specific to these sub-commands
	defaultConfig := internal.DefaultConfig(ctx)
	defaultKubeConfig := client.AuthConfig{}.ConfigPath()
	kubeDeployCmd.Flags().StringVarP(&podPostCreationScript, "script", "s", defaultConfig.CloudScript, "path to a start-up script that will be run on each pod created")
	kubeDeployCmd.Flags().IntVarP(&serverKeepAlive, "keepalive", "k", defaultConfig.CloudKeepAlive, "how long in seconds to keep idle spawned pods alive for; 0 means forever")
	kubeDeployCmd.Flags().IntVarP(&maxServers, "max_servers", "m", defaultConfig.CloudServers+1, "maximum number of pods to spawn; 0 means unlimited (default 0)")
	kubeDeployCmd.Flags().StringVarP(&podConfigFiles, "config_files", "c", defaultConfig.CloudConfigFiles, "comma separated paths of config files to copy to spawned pods")
	kubeDeployCmd.Flags().StringVarP(&containerImage, "container_image", "i", defaultConfig.ContainerImage, "image to use for spawned pods")
	kubeDeployCmd.Flags().StringVarP(&kubeNamespace, "namespace", "n", "", "use your own namespace (default random)")
	kubeDeployCmd.Flags().IntVarP(&managerTimeoutSeconds, "timeout", "t", 10, "how long to wait in seconds for the manager to start up")
	kubeDeployCmd.Flags().StringVarP(&kubeConfig, "kube_config", "", defaultKubeConfig, "the path to a kubeconfig file to authenticate with")
	kubeDeployCmd.Flags().BoolVar(&kubeDebug, "debug", false, "include extra debugging information in the logs")

	kubeTearDownCmd.Flags().BoolVarP(&forceTearDown, "force", "f", false, "force teardown even when the remote manager cannot be accessed")
	kubeTearDownCmd.Flags().StringVarP(&kubeNamespace, "namespace", "n", "", "operate on a specific namespace (default last deployed)")
	kubeTearDownCmd.Flags().StringVarP(&kubeConfig, "kube_config", "", defaultKubeConfig, "the path to a kubeconfig file to authenticate with")
	kubeTearDownCmd.Flags().BoolVar(&kubeDebug, "debug", false, "include extra debugging information in the logs")
}

// Rewrite any relative path to replace '~/' with podBinDir returning a
// []client.FilePair to be copied to the manager. The comma separated list is
// then passed again, and the same function called on the manager so all the
// filepaths should match up when the manager calls Spawn(). Currently only
// relative paths are allowed, any path not starting '~/' is dropped as
// everything ultimately needs to go into podBinDir as that's the volume that
// gets preserved across containers.
func rewriteConfigFiles(configFiles string) []client.FilePair {
	// Get current user's home directory
	hDir, herr := os.UserHomeDir()
	if herr != nil {
		warn("could not find home dir: %s", herr)
	}

	filePairs := []client.FilePair{}
	paths := []string{}

	// Get a slice of paths.
	split := strings.Split(configFiles, ",")

	// Loop over all paths in split, if any don't exist silently remove them.
	for _, path := range split {
		localPath := internal.TildaToHome(path)
		_, err := os.Stat(localPath)
		if err != nil {
			continue
		} else {
			paths = append(paths, path)
		}
	}

	// Remove the '~/' prefix as tar will create a ~/.. file. We don't want
	// this. Replace '~/' with podBinDir which we define as $HOME.
	dests := []string{}
	for _, path := range paths {
		if strings.HasPrefix(path, "~/") {
			// Return the file path relative to '~/'
			rel, err := filepath.Rel("~/", path)
			if err != nil {
				warn(fmt.Sprintf("Could not convert path %s to relative path.", path))
			}
			dir := filepath.Dir(rel)
			// Trim prefix dir = strings.TrimPrefix(dir, "~") Add podBinDir as
			// new prefix
			dir = podBinDir + dir + "/"
			dests = append(dests, dir+filepath.Base(path))
		} else {
			warn("File with path %s is being ignored as it does not have prefix '~/'", path)
		}
	}

	// Create []client.FilePair to pass in to the deploy options. Replace '~/'
	// with the current user's $HOME
	for i, path := range paths {
		if strings.HasPrefix(path, "~/") {
			// rewrite ~/ to hDir
			src := strings.TrimPrefix(path, "~/")
			src = hDir + "/" + src

			filePairs = append(filePairs, client.FilePair{Src: src, Dest: dests[i]})
		}
	}
	return filePairs
}

// Open a resource file with the provided path
func openResources(ctx context.Context, resourcePath string) (*cloud.Resources, error) {
	resources := &cloud.Resources{}
	file, err := os.Open(resourcePath)
	if err != nil {
		die("could not open resource file with path %s: %s", resourcePath, err)
	}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(resources)
	if err != nil {
		die("error decoding resource file %s: %s", resourcePath, err)
	}

	internal.LogClose(ctx, file, "resource file", "path", resourcePath)

	return resources, err
}
