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

// tests echo {42,24,mice,test} | xargs -n 1 -r echo echo | wr add

package maxcluster

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/kubernetes/client"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// Assumes that there is a wr deployment in existence in development mode. It
// then pulls the namespace from the resource file and runs the tests against
// the cluster there.

var tc client.Kubernetesp
var clientset kubernetes.Interface
var autherr error
var config *internal.Config
var token []byte
var jq *jobqueue.Client
var skip bool

func init() {
	ctx := context.Background()

	tc = client.Kubernetesp{}
	clientset, _, autherr = tc.Authenticate(ctx, client.AuthConfig{})
	if autherr != nil {
		skip = true
		return
	}

	_, autherr := clientset.CoreV1().Endpoints("default").List(metav1.ListOptions{})
	if autherr != nil {
		skip = true
		fmt.Printf("Failed to list endpoints for default namespace, assuming cluster connection failure.\n Skipping tests with error: %s\n", autherr)
		return
	}

	config = internal.ConfigLoadFromCurrentDir(ctx, internal.Development)
	resourcePath := filepath.Join(config.ManagerDir, "kubernetes_resources")
	resources := &cloud.Resources{}

	file, err := os.Open(resourcePath)
	if err != nil {
		fmt.Printf("No resource file found at %s, skipping tests.", resourcePath)
		skip = true
		return
	}

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(resources)
	if err != nil {
		panic(err)
	}

	token, err = os.ReadFile(config.ManagerTokenFile)
	if err != nil {
		panic(err)
	}

	jq, err = jobqueue.Connect(config.ManagerHost+":"+config.ManagerPort, config.ManagerCAFile, config.ManagerCertDomain, token, 15*time.Second)
	if err != nil {
		fmt.Printf("Failed to connect to jobqueue, skipping.")
		skip = true
		return
	}

	autherr = tc.Initialize(ctx, clientset, resources.Details["namespace"])
	if autherr != nil {
		skip = true
		return
	}
}

func TestClusterPend(t *testing.T) {
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	cases := []struct {
		repgrp string
	}{
		{
			repgrp: "max",
		},
	}
	for _, c := range cases {
		// Check the job can be found in the system, and that it has exited
		// successfully.
		var jobs []*jobqueue.Job
		var err error
		// The job may take some time to complete, so we need to poll.
		rg := c.repgrp
		errr := wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
			jobs, err = jq.GetByRepGroup(rg, false, 0, "", false, false)
			if err != nil {
				return false, err
			}

			// Wait for the jobs to be accepted into the queue
			if jobs == nil {
				return false, nil
			}

			// If there arent
			nodeList, errl := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
			if errl != nil {
				t.Errorf("Failed to list nodes: %s", errl)
			}

			// There should always be 2 * nodes jobs If not wait until
			// they're all in the list
			if len(jobs) != 2*len(nodeList.Items) {
				return false, nil
			}

			// For each job, ensure it runs & exits successfully.
			for _, job := range jobs {
				if job.Exited && job.Exitcode != 1 {
					t.Logf("cmd %s completed successfully.", job.Cmd)
				}
				if job.Exited && job.Exitcode == 1 {
					t.Errorf("cmd %s failed", job.Cmd)
					return false, fmt.Errorf("cmd failed")
				}
			}

			// All jobs have exited, we return true
			return true, nil
		})
		if errr != nil {
			t.Errorf("wait on jobs in rep group %s  failed: %s", c.repgrp, errr)
		}

		// Now check the pods are deleted after successful completion. They are
		// kept if they error.
		for _, job := range jobs {
			if len(job.Host) == 0 {
				t.Errorf("job %+v has no host.", job)
			}
			_, err = clientset.CoreV1().Pods(tc.NewNamespaceName).Get(job.Host, metav1.GetOptions{})
			if err != nil && errors.IsNotFound(err) {
				t.Logf("Success, pod %s with cmd %s deleted.", job.Host, job.Cmd)
			} else if err != nil {
				t.Errorf("Pod %s was not deleted: %s", job.Host, err)
			}
		}

	}
}
