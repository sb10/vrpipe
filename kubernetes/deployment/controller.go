// Copyright © 2018, 2021 Genome Research Limited
// Author: Theo Barber-Bany <tb15@sanger.ac.uk>.
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

package deployment

/*
Package deployment is a kubernetes controller to oversee the deployment of the
wr scheduler controller into a kubernetes cluster. It handles copying
configuration files and binaries as well as port forwarding.
*/

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/kubernetes/client"
	"github.com/inconshreveable/log15"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const maxRetries = 5

// Controller defines a deployment controller and it's options
type Controller struct {
	Client     *client.Kubernetesp
	Clientset  kubernetes.Interface
	Restconfig *rest.Config
	Opts       *DeployOpts
	queue      workqueue.RateLimitingInterface
	informer   cache.SharedIndexInformer
	log15.Logger
}

// DeployOpts specify the options for the deployment
type DeployOpts struct {
	ContainerImage  string             // docker hub image
	TempMountPath   string             // where to mount the binary
	Files           []client.FilePair  // Files to tar across
	BinaryPath      string             // full path to the binary to be executed
	BinaryArgs      []string           // arguments to the binary
	ConfigMapName   string             // name of the configmap to execute
	ConfigMountPath string             // path to the configmap
	RequiredPorts   []int              // ports that require forwarding
	AttachCmdOpts   *client.CmdOptions // relevant options for the attachCmd
	ResourcePath    string             // Path to the resource file kubeCmd creates
	Logger          log15.Logger
}

func (c *Controller) createQueueAndInformer() {
	c.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	c.informer = cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return c.Clientset.CoreV1().Pods(c.Client.NewNamespaceName).List(metav1.ListOptions{
					LabelSelector: "app=wr-manager",
				})

			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return c.Clientset.CoreV1().Pods(c.Client.NewNamespaceName).Watch(metav1.ListOptions{
					LabelSelector:        "app=wr-manager",
					IncludeUninitialized: true,
					Watch:                true,
				})
			},
		},
		&apiv1.Pod{},
		0, //Skip resync
		cache.Indexers{},
	)
}

func (c *Controller) addEventHandlers() {
	c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				c.queue.Add(key)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				c.queue.Add(key)
			}

		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				c.queue.Add(key)
			}
		},
	})
}

// HasSynced is required for cache.Controller interface.
func (c *Controller) HasSynced() bool {
	return c.informer.HasSynced()
}

// Run starts SharedInformer watching for pods, and sends their keys to
// workqueue. StopCh used to send interrupt.
func (c *Controller) Run(ctx context.Context, stopCh <-chan struct{}) {
	c.Logger = c.Opts.Logger.New("deployment", "kubernetes")
	c.createQueueAndInformer()
	c.addEventHandlers()
	// don't let panics crash the process
	defer utilruntime.HandleCrash()
	// Ensure workqueue is shut down properly
	defer c.queue.ShutDown()

	go c.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	// keep processing items until we are stopped
	go c.processItems(ctx, stopCh)
	<-stopCh
}

// processItems calls processNextItem() repeatedly until the given chan is
// closed.
func (c *Controller) processItems(ctx context.Context, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			c.processNextItem(ctx)
		}
	}
}

func (c *Controller) processNextItem(ctx context.Context) {
	// pull next key from queue. Look up key in cache.
	key, quit := c.queue.Get()
	if quit {
		// *** currently we don't care about this and just keep calling this
		// method during processItems(); don't know if this is correct, but
		// seemed to be the original author's intent
		return
	}

	// Indicate to queue key has been processed
	defer c.queue.Done(key)

	// do processing on key
	err := c.processItem(ctx, key.(string))
	if err != nil {
		if c.queue.NumRequeues(key) < maxRetries {
			c.Error("processing queue item, will retry", "key", key, "error", err)
			// requeue
			c.queue.AddRateLimited(key)
			return
		}

		// too many retries
		c.Error("processing queue item failed", "key", key, "error", err)
		c.queue.Forget(key)
		utilruntime.HandleError(err)
		return
	}

	// No error => queue stop tracking history
	c.queue.Forget(key)
}

// processItem(key) is where we define how to react to an item coming off the
// work queue.
func (c *Controller) processItem(ctx context.Context, key string) error {
	c.Debug("processing change to pod", "pod", key)

	obj, exists, err := c.informer.GetIndexer().GetByKey(key)
	if err != nil {
		return fmt.Errorf("error fetching object with key %s from store: %v", key, err)
	}

	if !exists {
		c.Debug("object deleted", "key", key, "obj", obj)
		return nil
	}
	err = c.processObj(ctx, obj)
	return err
}

// Process a generic object.
func (c *Controller) processObj(ctx context.Context, obj interface{}) error {
	switch v := obj.(type) {
	case *apiv1.Pod:
		c.processPod(ctx, v)
	default:
		return fmt.Errorf("obj is not a pod")
	}
	return nil
}

// processPod defines how to react to a pod coming off the workqueue in an
// observed state. Assumes there is only 1 initcontainer.
func (c *Controller) processPod(ctx context.Context, obj *apiv1.Pod) {
	if len(obj.Status.InitContainerStatuses) >= 1 {
		switch {
		case obj.Status.InitContainerStatuses[0].State.Waiting != nil:
			c.Debug("init container waiting!")
		case obj.Status.InitContainerStatuses[0].State.Running != nil:
			c.Debug("init container running, calling CopyTar", "files", c.Opts.Files)
			err := c.Client.CopyTar(ctx, c.Opts.Files, obj)
			if err != nil {
				c.Error("copying tarball", "error", err)
			}
		case obj.Status.ContainerStatuses[0].State.Running != nil:
			// Write the pod name, name to the resources file. This allows us to
			// retrieve it to obtain the client.token
			resources := &cloud.Resources{}
			file, err := os.OpenFile(c.Opts.ResourcePath, os.O_RDONLY, 0600)
			if err != nil {
				c.Error("could not open resource file", "path", c.Opts.ResourcePath, "error", err)
				return
			}
			c.Debug("opened resource file", "path", c.Opts.ResourcePath)
			decoder := gob.NewDecoder(file)
			err = decoder.Decode(resources)
			if err != nil {
				c.Error("decoding resource file", "error", err)
				return
			}
			err = file.Close()
			if err != nil {
				c.Error("failed to close resource file", "error", err)
			}
			file2, err := os.OpenFile(c.Opts.ResourcePath, os.O_WRONLY, 0600)
			if err != nil {
				c.Error("failed to open file2", "error", err)
			}
			resources.Details["manager-pod"] = obj.ObjectMeta.Name
			encoder := gob.NewEncoder(file2)
			err = encoder.Encode(resources)
			if err != nil {
				c.Error("failed to encode resource file", "error", err)
			}
			err = file2.Close()
			if err != nil {
				c.Error("failed to close resource file2", "error", err)
			}

			// If everything went well, log.
			if err == nil {
				c.Debug("stored manager pod name in resource file", "name", obj.ObjectMeta.Name)
			}

			c.Info("wr manager container is running, calling PortForward", "ports", c.Opts.RequiredPorts)
			go func() {
				err := c.Client.PortForward(ctx, obj, c.Opts.RequiredPorts)
				if err != nil {
					c.Error("Port forwarding error", "error", err)
				}
			}()
		}
	} else {
		c.Debug("initContainerStatuses not initialised yet")
	}
}
