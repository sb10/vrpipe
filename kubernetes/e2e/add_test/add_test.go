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

package addtest

import (
	"context"
	"crypto/md5"
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
	apiv1 "k8s.io/api/core/v1"
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

func TestEchoes(t *testing.T) {
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	t.Parallel()
	cases := []struct {
		cmd string
	}{
		{
			cmd: "echo 42",
		},
		{
			cmd: "echo 24",
		},
		{
			cmd: "echo mice",
		},
		{
			cmd: "echo test",
		},
	}
	for _, c := range cases {
		// Check the job can be found in the system, and that it has exited
		// successfully.
		var job *jobqueue.Job
		var err error
		// The job may take some time to complete, so we need to poll.
		cmd := c.cmd
		errr := wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
			job, err = jq.GetByEssence(&jobqueue.JobEssence{Cmd: cmd}, false, false)
			if err != nil {
				return false, err
			}
			if job == nil {
				return false, nil
			}
			return checkJob(job)
		})
		if errr != nil {
			t.Errorf("wait on cmd %s completion failed: %s", c.cmd, errr)
		}

		// Now check the pods are deleted after successful completion. They are
		// kept if they error.
		_, err = clientset.CoreV1().Pods(tc.NewNamespaceName).Get(job.Host, metav1.GetOptions{})
		if err != nil && errors.IsNotFound(err) {
			t.Logf("Success, pod %s with cmd %s deleted.", job.Host, job.Cmd)
		} else if err != nil {
			t.Errorf("Pod %s was not deleted: %s", job.Host, err)
		}
	}
}
func TestFileCreation(t *testing.T) {
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	t.Parallel()
	cases := []struct {
		cmd string
	}{
		{
			cmd: "echo hello world > /tmp/hw",
		},
	}
	for _, c := range cases {
		// Check the job can be found in the system, and that it has exited
		// successfully.
		var job *jobqueue.Job
		var err error
		// The job may take some time to complete, so we need to poll.
		cmd := c.cmd
		errr := wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout*2, func() (bool, error) {
			job, err = jq.GetByEssence(&jobqueue.JobEssence{Cmd: cmd}, false, false)
			if err != nil {
				return false, err
			}
			if job == nil {
				return false, nil
			}
			return checkJob(job)
		})
		if errr != nil {
			t.Errorf("wait on cmd '%s' completion failed: %s. WR error (If available): %s", c.cmd, errr, job.FailReason)
		}

		// Now we get the host, and copy the file to memory. Then calculate the
		// md5 of the file. (Verification step)
		stdout, _, err := tc.ExecInPod(job.Host, "wr-runner", tc.NewNamespaceName, []string{"cat", "/tmp/hw"})
		if err != nil {
			t.Errorf("Failed to get file from container: %s", err)
		}

		expectedMd5 := "6f5902ac237024bdd0c176cb93063dc4"

		md5 := fmt.Sprintf("%x", md5.Sum([]byte(stdout)))

		if md5 != expectedMd5 {
			t.Errorf("MD5 do not match expected : %s, got: %s", expectedMd5, md5)
		}

		// Clean up manually. This is because we have a limited number of cores
		// in CI, and everything will time out if we don't.
		err = clientset.CoreV1().Pods(tc.NewNamespaceName).Delete(job.Host, &metav1.DeleteOptions{})
		if err != nil {
			t.Errorf("Failed to delete pod %s: %s", job.Host, err)
		}
	}
}

func TestContainerImage(t *testing.T) {
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	t.Parallel()
	cases := []struct {
		cmd            string
		containerImage string
	}{
		{
			cmd:            "echo golang:latest",
			containerImage: "golang:latest",
		},
		{
			cmd:            "echo genomicpariscentre/samtools",
			containerImage: "genomicpariscentre/samtools",
		},
	}
	for _, c := range cases {
		// Check the job can be found in the system, and that it has exited
		// successfully.
		var job *jobqueue.Job
		var err error

		// The job may take some time to complete, so we need to poll.
		cmd := c.cmd
		errr := wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout*2, func() (bool, error) {
			job, err = jq.GetByEssence(&jobqueue.JobEssence{Cmd: cmd}, false, false)
			if err != nil {
				return false, err
			}
			if job == nil {
				return false, nil
			}
			return checkJob(job)
		})
		if errr != nil {
			t.Errorf("wait on cmd '%s' completion failed: %s. WR error (If available): %s", c.cmd, errr, job.FailReason)
		}

		// Now the job has completed successfully we check that the image used is
		// as expected
		pod, err := clientset.CoreV1().Pods(tc.NewNamespaceName).Get(job.Host, metav1.GetOptions{})
		if err != nil {
			t.Errorf("Getting pod failed %s", err)
		}

		if pod.Status.Phase == apiv1.PodFailed {
			t.Errorf("Pod %s failed", pod.ObjectMeta.Name)
		}

		var runnercontainer *apiv1.Container
		for _, container := range pod.Spec.Containers {
			if container.Name == "wr-runner" {
				c := container
				runnercontainer = &c
				break
			}
		}

		if runnercontainer == nil {
			t.Fatalf("Failed to find runner container in pod %s", pod.ObjectMeta.Name)
		}

		if runnercontainer.Image != c.containerImage {
			t.Errorf("Unexpected container image for runner %s, expected %s got %s", pod.ObjectMeta.Name, c.containerImage, runnercontainer.Image)
		}

		// Clean up manually. This is because we have a limited number of cores
		// in CI, and everything will time out if we don't.
		err = clientset.CoreV1().Pods(tc.NewNamespaceName).Delete(job.Host, &metav1.DeleteOptions{})
		if err != nil {
			t.Errorf("Failed to delete pod %s: %s", job.Host, err)
		}
	}
}

// checkJob checks if the passed job has exited 0. If it has not it gets the
// STDERR of the job and returns it.
func checkJob(job *jobqueue.Job) (bool, error) {
	if job.Exited && job.Exitcode != 1 {
		return true, nil
	}
	if job.Exited && job.Exitcode == 1 {
		stdErr, err := job.StdErr()
		if err != nil {
			return false, fmt.Errorf("Job failed, and failed to get stderr")
		}

		return false, fmt.Errorf("cmd cmd %s failed: %s", job.Cmd, stdErr)
	}
	return false, nil
}
