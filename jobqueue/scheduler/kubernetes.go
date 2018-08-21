// Copyright © 2018 Genome Research Limited Author: Theo Barber-Bany
// <tb15@sanger.ac.uk>.
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

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/kubernetes/client"
	kubescheduler "github.com/VertebrateResequencing/wr/kubernetes/scheduler"
	"github.com/VertebrateResequencing/wr/queue"
	"github.com/inconshreveable/log15"
	"github.com/sb10/l15h"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"

	"strings"
	"sync"
	"time"
)

// k8s is the implementer of scheduleri. it is a wrapper to implement scheduleri
// by sending requests to the controller

// maxQueueTime(), reserveTimeout(), hostToID(), busy(), schedule() are
// inherited from local
type k8s struct {
	local
	config          *ConfigKubernetes
	libclient       *client.Kubernetesp
	callBackChan    chan string
	cbmutex         *sync.RWMutex
	badCallBackChan chan *cloud.Server
	reqChan         chan *kubescheduler.Request
	podAliveChan    chan *kubescheduler.PodAlive
	msgCB           MessageCallBack
	badServerCB     BadServerCallBack
	es              bool // Does the cluster support ephemeral storage reporting?
	esmutex         *sync.RWMutex
	log15.Logger
}

var defaultScriptName = client.DefaultScriptName
var configMapName string
var scriptName string

const kubeSchedulerLog = "kubeSchedulerLog"

// ConfigKubernetes holds the configuration options for the kubernetes wr driver
type ConfigKubernetes struct {
	// The image name (Docker Hub) to pull to run wr Runners with. Defaults to
	// 'ubuntu:latest'
	Image string

	// By default, containers in pods run as root, to run as a different user,
	// specify here.
	User string

	// Requested RAM, a pod will default to 64m, and be allocated more up to a
	// limit
	RAM int

	// Requested Disk space, in GB Currently not implemented: Exploiting node
	// ephemeral storage
	Disk int

	// PostCreationScript is the []byte content of a script you want executed
	// after a server is Spawn()ed. (Overridden during Schedule() by a
	// Requirements.Other["cloud_script"] value.)
	PostCreationScript []byte

	// ConfigMap to use in place of PostCreationScript
	ConfigMap string

	// ConfigFiles is a comma separated list of paths to config files that
	// should be copied over to all spawned servers. Absolute paths are copied
	// over to the same absolute path on the new server. To handle a config file
	// that should remain relative to the home directory (and where the spawned
	// server may have a different username and thus home directory path
	// compared to the current server), use the prefix ~/ to signify the home
	// directory. It silently ignores files that don't exist locally.
	ConfigFiles string

	// DNSNameServers specifies any additional DNS Nameservers to use by default
	// kubernetes uses kubedns, and those set at cluster deployment which will
	// be set by a cluster administrator. See
	// https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-config
	// for more details
	DNSNameServers []string

	// Please only use bash for now!
	Shell string

	// StateUpdateFrequency is the frequency at which to check spawned servers
	// that are being used to run things, to see if they're still alive. 0
	// (default) is treated as 1 minute.
	StateUpdateFrequency time.Duration

	// Namespace to initialise clientsets to
	Namespace string

	// TempMountPath defines the path at which to copy the wr binary to in the
	// container. It points to an empty volume shared between the init container
	// and main container and is where copied files are stored. It should always
	// be the same as what's currently running on the manager otherwise the cmd
	// passed to runCmd() will be incorrect. Also defined as $HOME
	TempMountPath string

	// LocalBinaryPath points to where the wr binary will be accessed to copy to
	// each pod. It should be generated by the invoking command.
	LocalBinaryPath string

	// Manager Directory to log to
	ManagerDir string

	// Debug mode sets requested resources at 1/10th the integer value. useful
	// for testing.
	Debug bool
}

// AddConfigFile takes a value as per the ConfigFiles property, and appends it
// to the existing ConfigFiles value (or sets it if unset).
func (c *ConfigKubernetes) AddConfigFile(configFile string) {
	if c.ConfigFiles == "" {
		c.ConfigFiles = configFile
	} else {
		c.ConfigFiles += "," + configFile
	}
}

// Set up prerequisites, call Run() Create channels to pass requests to the
// controller. Create queue.
func (s *k8s) initialize(config interface{}, logger log15.Logger) error {
	s.config = config.(*ConfigKubernetes)

	s.Logger = logger.New("scheduler", "kubernetes")
	kubeLogFile := filepath.Join(s.config.ManagerDir, kubeSchedulerLog)
	fh, err := log15.FileHandler(kubeLogFile, log15.LogfmtFormat())
	if err != nil {
		return fmt.Errorf("wr kubernetes scheduler could not log to %s: %s", kubeLogFile, err)
	}

	l15h.AddHandler(s.Logger, fh)

	s.Debug("configuration passed", "configuration", s.config)

	// make queue
	s.queue = queue.New(localPlace)
	s.running = make(map[string]int)

	// set our functions for use in schedule() and processQueue()
	s.reqCheckFunc = s.reqCheck
	s.canCountFunc = s.canCount
	s.runCmdFunc = s.runCmd
	s.cancelRunCmdFunc = s.cancelRun
	s.stateUpdateFunc = s.stateUpdate
	s.maxMemFunc = s.maxMem
	s.maxCPUFunc = s.maxCPU
	s.cbmutex = new(sync.RWMutex)
	s.esmutex = new(sync.RWMutex)
	s.stateUpdateFreq = s.config.StateUpdateFrequency
	if s.stateUpdateFreq == 0 {
		s.stateUpdateFreq = 1 * time.Minute
	}

	// pass through our shell config and logger to our local embed
	s.local.config = &ConfigLocal{Shell: s.config.Shell}
	s.local.Logger = s.Logger

	// Create the default PostCreationScript if no config map passed. If the
	// byte stream does not stringify things may go horribly wrong.
	if len(s.config.ConfigMap) == 0 {
		if string(s.config.PostCreationScript) != "" {
			cmap, err := s.libclient.CreateInitScriptConfigMap(string(s.config.PostCreationScript))
			if err != nil {
				s.Crit("failed to create configmap from PostCreationScript")
				return err
			}
			configMapName = cmap.ObjectMeta.Name
			scriptName = defaultScriptName
		} else {
			s.Crit("a config map or post creation script must be provided.")
		}
	}

	// Set up message notifier & request channels
	s.callBackChan = make(chan string, 5)
	s.badCallBackChan = make(chan *cloud.Server, 5)
	s.reqChan = make(chan *kubescheduler.Request)
	s.podAliveChan = make(chan *kubescheduler.PodAlive)

	if s.msgCB == nil {
		s.Warn("No message callback function set")
	}
	if s.badServerCB == nil {
		s.Warn("No bad server callback function set")
	}

	// Prerequisites to start the controller
	s.libclient = &client.Kubernetesp{}
	kubeClient, restConfig, err := s.libclient.Authenticate(s) // Authenticate against the cluster.
	if err != nil {
		return err
	}
	// Initialise all internal clients on  the provided namespace
	err = s.libclient.Initialize(kubeClient, s.config.Namespace)
	if err != nil {
		s.Crit("failed to initialise the internal clients to namespace", "namespace", s.config.Namespace, "error", err)
		panic(err)
	}

	// Initialise the informer factory Confine all informers to the provided
	// namespace
	kubeInformerFactory := kubeinformers.NewFilteredSharedInformerFactory(kubeClient, time.Second*15, s.config.Namespace, func(listopts *metav1.ListOptions) {
		listopts.IncludeUninitialized = true
		//listopts.Watch = true
	})

	// Rewrite config files.
	files := s.rewriteConfigFiles(s.config.ConfigFiles)
	files = append(files, client.FilePair{Src: s.config.LocalBinaryPath, Dest: s.config.TempMountPath})

	// Initialise scheduler opts
	opts := kubescheduler.ScheduleOpts{
		Files:        files,
		CbChan:       s.callBackChan,
		BadCbChan:    s.badCallBackChan,
		ReqChan:      s.reqChan,
		PodAliveChan: s.podAliveChan,
		Logger:       logger,
		ManagerDir:   s.config.ManagerDir,
	}

	// Start listening for messages on call back channels
	go s.notifyCallBack(s.callBackChan, s.badCallBackChan)

	// Create the controller
	controller := kubescheduler.NewController(kubeClient, restConfig, s.libclient, kubeInformerFactory, opts)
	s.Debug("Controller contents", "contents", controller)
	stopCh := make(chan struct{})

	go kubeInformerFactory.Start(stopCh)

	// Start the scheduling controller
	s.Debug("Starting scheduling controller")
	go func() {
		if err = controller.Run(2, stopCh); err != nil {
			s.Error("Error running controller", "error", err.Error())
		}
	}()

	return nil
}

// Send a request to see if a cmd with the provided requirements can ever be
// scheduled. If the request can be scheduled, errChan returns nil then is
// closed If it can't ever be sheduled an error is sent on errChan and returned.
// TODO: OCC if error: What if a node is added shortly after? (Deals with
// autoscaling?)
// https://godoc.org/k8s.io/apimachinery/pkg/util/wait#ExponentialBackoff
func (s *k8s) reqCheck(req *Requirements) error {
	s.Debug("reqCheck called with requirements", "requirements", req)

	cores, ram, disk := s.generateResourceRequests(req)

	r := &kubescheduler.Request{
		RAM:    *ram,
		Time:   req.Time,
		Cores:  *cores,
		Disk:   *disk,
		Other:  req.Other,
		CbChan: make(chan kubescheduler.Response),
	}

	s.Debug("Sending request to listener", "request", r)
	go func() {
		s.reqChan <- r
	}()

	s.Debug("Waiting on reqCheck to return")
	resp := <-r.CbChan

	s.esmutex.Lock()
	defer s.esmutex.Unlock()

	if resp.Error != nil {
		s.Error("Requirements check recieved error", "error", resp.Error)
		s.es = resp.Ephemeral
		return resp.Error
	}

	s.Debug("reqCheck returned ok")
	s.es = resp.Ephemeral

	return resp.Error
}

// setMessageCallBack sets the given callback function.
func (s *k8s) setMessageCallBack(cb MessageCallBack) {
	s.Debug("setMessageCallBack called")
	s.cbmutex.Lock()
	defer s.cbmutex.Unlock()
	s.msgCB = cb
}

// setBadServerCallBack sets the given callback function.
func (s *k8s) setBadServerCallBack(cb BadServerCallBack) {
	s.Debug("setBadServerCallBack called")
	s.cbmutex.Lock()
	defer s.cbmutex.Unlock()
	s.badServerCB = cb
}

// The controller is passed a callback channel. notifyMessage recieves on the
// channel if anything is recieved call s.msgCB(msg).
func (s *k8s) notifyCallBack(callBackChan chan string, badCallBackChan chan *cloud.Server) {
	s.Debug("notifyCallBack handler started")
	for {
		select {
		case msg := <-callBackChan:
			s.Debug("Callback notification", "msg", msg)
			if s.msgCB != nil {
				go s.msgCB(msg)
			}
		case badServer := <-badCallBackChan:
			s.Debug("Bad server callback notification", "msg", badServer)
			if s.badServerCB != nil {
				go s.badServerCB(badServer)
			}
		}
	}

}

func (s *k8s) cleanup() {
	s.Debug("cleanup() Called")

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.cleaned = true
	err := s.queue.Destroy()
	if err != nil {
		s.Warn("cleanup queue destruction failed", "err", err)
	}

}

// Work out how many pods with given resource requests can be scheduled based on
// resource requests on the nodes in the cluster.
func (s *k8s) canCount(req *Requirements) (canCount int) {
	s.Debug("canCount Called, returning 100")
	// 100 is  a big enough block for anyone...
	return 100
}

// RunFunc calls spawn() and exits with an error = nil when pod has terminated.
// (Runner exited) Or an error if there was a problem. Use deletefunc in
// controller to send message? (based on some sort of channel communication?)
func (s *k8s) runCmd(cmd string, req *Requirements, reservedCh chan bool) error {
	s.Debug("RunCmd Called", "cmd", cmd, "requirements", req)
	// The first 'argument' to cmd will be the absolute path to the manager's
	// executable. Work out the local binary's name from localBinaryPath.
	// binaryName := filepath.Base(s.config.localBinaryPath)

	configMountPath := "/scripts"

	// If there is an overwridden cloud_script, create a configmap and pass that
	// instead. If there was a ConfigMap passed
	if val, defined := req.Other["cloud_script"]; defined {
		cmap, err := s.libclient.CreateInitScriptConfigMap(val)
		if err != nil {
			return err
		}
		configMapName = cmap.ObjectMeta.Name
		scriptName = defaultScriptName
	} else if len(s.config.ConfigMap) != 0 {
		configMapName = s.config.ConfigMap
		scriptName = defaultScriptName
	}

	// If there is an overwridden cloud_os, pass that instead.
	var containerImage string
	if val, defined := req.Other["cloud_os"]; defined {
		containerImage = val
		s.Debug("setting container image", "container image", containerImage)
	} else {
		containerImage = s.config.Image
	}

	// Remove any single quotes as this causes issues passing information to the
	// runner
	cmd = strings.Replace(cmd, "'", "", -1)
	binaryArgs := []string{cmd}

	resources := s.generateResourceRequirements(req)

	// If ephemeral storage is not enabled on the cluster don't request any
	s.esmutex.RLock()
	if !s.es {
		resources.Requests[apiv1.ResourceEphemeralStorage] = *resource.NewQuantity(int64(0)*1024*1024*1024, resource.BinarySI)
	}
	s.esmutex.RUnlock()

	//DEBUG: binaryArgs = []string{"tail", "-f", "/dev/null"}

	s.Debug("Spawning pod with requirements", "requirements", resources)
	pod, err := s.libclient.Spawn(containerImage,
		s.config.TempMountPath,
		configMountPath+"/"+scriptName,
		binaryArgs,
		configMapName,
		configMountPath,
		resources)

	if err != nil {
		s.Error("error spawning runner pod", "err", err)
		s.msgCB(fmt.Sprintf("unable to spawn a runner with requirements %s: %s", req.Stringify(), err))
		reservedCh <- false
		return err
	}

	reservedCh <- true
	s.Debug("Spawn request succeded", "pod", pod.ObjectMeta.Name)

	// We need to know when the pod we've created (the runner) terminates there
	// is a listener in the controller that will notify when a pod passed to it
	// as a request containing a name and channel is deleted. The notification
	// is the channel being closed.

	// Send the request to the listener.
	s.Debug("Sending request to the podAliveChan", "pod", pod.ObjectMeta.Name)
	errChan := make(chan error)
	go func() {
		req := &kubescheduler.PodAlive{
			Pod:     pod,
			ErrChan: errChan,
			Done:    false,
		}
		s.podAliveChan <- req
	}()

	// Wait for the response, if there is an error e.g CrashBackLoopoff
	// suggesting the post create script is throwing an error, return it here.
	// Don't delete the pod if some error is thrown.
	s.Debug("Waiting on status of pod", "pod", pod.ObjectMeta.Name)
	err = <-errChan
	if err != nil {
		s.Error("error with pod", "pod", pod.ObjectMeta.Name, "err", err)
		return err
	}
	// Delete terminated pod if no error thrown.
	s.Debug("Deleting pod", "pod", pod.ObjectMeta.Name)
	err = s.libclient.DestroyPod(pod.ObjectMeta.Name)
	s.Debug("Returning at end of runCmd()")

	return err
}

// This is ugly, I'm sorry. * Run on the manager, inside the cluster * Rewrite
// any relative path to replace '~/' with TempMountPath returning
// []client.FilePair to be copied to the runner. currently only relative paths
// are allowed, any path not starting '~/' is dropped as everything ultimately
// needs to go into TempMountPath as that's the volume that gets preserved
// across containers.
func (s *k8s) rewriteConfigFiles(configFiles string) []client.FilePair {
	// Get current user's home directory os.user.Current() was failing in a pod.
	// https://github.com/mitchellh/go-homedir ?
	hDir := os.Getenv("HOME")
	filePairs := []client.FilePair{}
	paths := []string{}
	pairSrc := []string{}
	pairDst := []string{}

	// Get a slice of paths.
	split := strings.Split(configFiles, ",")

	// Loop over all local paths in split, if any don't exist silently remove
	// them. If a path is of the form src:dest stat the src, if it exists add
	// the pair to a new set to be processed separately. It probably makes more
	// sense to create the []FilePair here, and just process all FilePair.Dests
	// in one step at the end.
	for _, path := range split {
		localPath := internal.TildaToHome(path)
		_, err := os.Stat(localPath)
		if err != nil {
			// Assume first that path is of the form src:dest
			srcDest := strings.Split(path, ":")
			// If there is no : separator, drop the path
			if len(srcDest) == 1 {
				s.Warn("Dropping path", "path", localPath, "error", err)
				continue
			}
			if len(srcDest) == 2 {
				// the client.token is not generated when this runs, so if the
				// file is client.token, ignore the fact it does not exist
				if filepath.Base(srcDest[0]) == "client.token" {
					s.Debug("Adding client token", "path", path)
					pairSrc = append(pairSrc, srcDest[0])
					pairDst = append(pairDst, srcDest[1])
					continue
				} else {
					// Check the src exists.
					_, errr := os.Stat(srcDest[0])
					if errr != nil {
						s.Warn("Dropping path", "path", path, "error", errr)
						continue
					}

					// Add the src:dest to separate slices
					pairSrc = append(pairSrc, srcDest[0])
					pairDst = append(pairDst, srcDest[1])
					continue
				}
			}
			s.Error("Source destination pair malformed", "pair", path)

		} else {
			paths = append(paths, path)
		}
	}

	// create []client.FilePair to pass in to the deploy options. Replace '~/'
	// with the current user's $HOME. This is because $HOME on the manager will
	// match $HOME on the spawned runner.

	// Process single paths
	dests := s.rewriteDests(paths)

	// For all paths which the src check has passed create the dest path by
	// rewriting ~/ to hDir.
	for i, path := range paths {
		if strings.HasPrefix(path, "~/") {
			// rewrite ~/ to hDir
			st := strings.TrimPrefix(path, "~/")
			st = hDir + st

			filePairs = append(filePairs, client.FilePair{Src: st, Dest: dests[i]})
		} else {
			// The source must exist, so tar won't fail. Add it anyway.
			filePairs = append(filePairs, client.FilePair{Src: path, Dest: dests[i]})
		}
	}

	// Process src:dest pairs
	dests = s.rewriteDests(pairDst)

	// For all paths which the src check has passed create the dest path by
	// rewriting ~/ to hDir.
	for i, path := range pairSrc {
		if strings.HasPrefix(path, "~/") {
			// rewrite ~/ to hDir
			st := strings.TrimPrefix(path, "~/")
			st = hDir + st

			filePairs = append(filePairs, client.FilePair{Src: st, Dest: dests[i]})
		} else {
			// The source must exist, so tar won't fail. Add it anyway.
			filePairs = append(filePairs, client.FilePair{Src: path, Dest: dests[i]})
		}
	}
	return filePairs

}

// remove the '~/' prefix as tar will create a ~/.. file. We don't want this as
// the files will be lost in the initcontainers filesystem. replace '~/' with
// TempMountPath which we define as $HOME in the created pods. Remove the file
// name, just returning the directory it is in.
func (s *k8s) rewriteDests(paths []string) []string {
	dests := []string{}
	for _, path := range paths {
		if strings.HasPrefix(path, "~/") {
			// Return the file path relative to '~/'
			rel, err := filepath.Rel("~/", path)
			if err != nil {
				s.Error("Could not convert path to relative path.", "path", path)
			}
			dir := filepath.Dir(rel)
			// Trim prefix dir = strings.TrimPrefix(dir, "~") Add podBinDir as
			// new prefix
			dir = s.config.TempMountPath + dir + "/"
			dests = append(dests, dir)
		} else {
			s.Warn("File may be lost as it does not have prefix '~/'", "file", path)
			dests = append(dests, path)
		}
	}
	return dests
}

// Tell the embedded local scheduler that we've got unlimited resources. Let the
// k8s scheduler handle everything else. ~~ It may be worth expanding this to
// set maximums to the total node capacity at some point. ~~
func (s *k8s) maxMem() int {
	return 0
}

func (s *k8s) maxCPU() int {
	return 0
}

// Rewrite *Requirements to a kubescheduler.Request Adjust values if debug
// enabled.
func (s *k8s) generateResourceRequests(req *Requirements) (*resource.Quantity, *resource.Quantity, *resource.Quantity) {
	var coreMult int64
	var diskMult int64

	coreMult = 1000
	diskMult = 1024

	if s.config.Debug {
		coreMult = 100
		diskMult = 1
	}

	cores := resource.NewMilliQuantity(int64(req.Cores)*coreMult, resource.DecimalSI)
	ram := resource.NewQuantity(int64(req.RAM)*1024*1024, resource.BinarySI)
	disk := resource.NewQuantity(int64(req.Disk)*1024*1024*diskMult, resource.BinarySI)

	return cores, ram, disk
}

// Build ResourceRequirements for Spawn() Adjust values if debug enabled.
func (s *k8s) generateResourceRequirements(req *Requirements) apiv1.ResourceRequirements {
	coreMult := int64(1000)

	if s.config.Debug {
		coreMult = 100
	}

	cores, ram, disk := s.generateResourceRequests(req)

	resources := apiv1.ResourceRequirements{
		Requests: apiv1.ResourceList{
			apiv1.ResourceCPU:              *cores,
			apiv1.ResourceMemory:           *ram,
			apiv1.ResourceEphemeralStorage: *disk,
		},
		Limits: apiv1.ResourceList{
			apiv1.ResourceCPU:    *resource.NewMilliQuantity(int64(req.Cores+1)*coreMult, resource.DecimalSI),
			apiv1.ResourceMemory: *resource.NewQuantity(int64(req.RAM+(req.RAM/5))*1024*1024, resource.BinarySI),
		},
	}
	return resources
}
