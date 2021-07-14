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

package client_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/kubernetes/client"
	"github.com/docker/docker/pkg/namesgenerator"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

var tc client.Kubernetesp
var clientset kubernetes.Interface
var autherr error
var nsErr error
var testingNamespace string
var skip bool

func init() {
	ctx := context.Background()
	tc = client.Kubernetesp{}
	clientset, _, autherr = tc.Authenticate(ctx, client.AuthConfig{})
	if autherr != nil {
		skip = true
		return
	}

	rand.Seed(time.Now().UnixNano())
	testingNamespace = strings.Replace(namesgenerator.GetRandomName(1), "_", "-", -1) + "-wr-testing"

	nsErr = tc.CreateNewNamespace(testingNamespace)
	if nsErr != nil {
		fmt.Printf("Failed to create namespace: %s", nsErr)
		skip = true
		return
	}

	autherr = tc.Initialize(ctx, clientset, testingNamespace)

	_, autherr := clientset.CoreV1().Endpoints(testingNamespace).List(metav1.ListOptions{})
	if autherr != nil {
		skip = true
		fmt.Printf("Failed to list endpoints for testing namespace, assuming cluster connection failure.\n Skipping tests with error: %s\n", autherr)
	}
}

func TestCreateNewNamespace(t *testing.T) {
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	cases := []struct {
		namespaceName string
	}{
		{
			namespaceName: "test",
		},
	}
	for _, c := range cases {
		err := tc.CreateNewNamespace(c.namespaceName)
		if err != nil {
			t.Error(err.Error())
		}
		_, err = clientset.CoreV1().Namespaces().Get(c.namespaceName, metav1.GetOptions{})
		if err != nil {
			t.Error(err.Error())
		}
		// Clean up
		err = clientset.CoreV1().Namespaces().Delete(c.namespaceName, &metav1.DeleteOptions{})
		if err != nil {
			t.Log("failed to clean up namespace", err.Error())
		}
	}
}

// Test that the Deploy() call returns without error.
func TestDeploy(t *testing.T) {
	ctx := context.Background()
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	cases := []struct {
		containerImage  string
		tempMountPath   string
		cmdArgs         []string
		configMountPath string
		configMapData   string
		requiredPorts   []int
	}{
		{
			containerImage:  "ubuntu:latest",
			tempMountPath:   "/wr-tmp/",
			cmdArgs:         []string{"tail", "-f", "/dev/null"},
			configMountPath: "/scripts/",
			configMapData:   "echo \"hello world\"",
			requiredPorts:   []int{80, 8080},
		},
	}
	for _, c := range cases {
		// Test the creation of config maps (2 birds one stone) We won't delete
		// this now so we can use it later.
		configmap, err := tc.CreateInitScriptConfigMap(c.configMapData)
		if err != nil {
			t.Error(err.Error())
		}

		expectedData := "#!/usr/bin/env bash\nset -euo pipefail\necho \"Running init script\"" +
			"\necho \"hello world\"\necho \"Init Script complete, executing arguments provided\"\nexec $@"

		if configmap.Data[client.DefaultScriptName] != expectedData {
			t.Error(fmt.Errorf("Unexpected contents of config map, got:\n%s \nexpect:\n%s", configmap.Data[client.DefaultScriptName], expectedData))
		}

		// Create the deployment we run the init script created from wherever
		// we've decided to mount it.
		err = tc.Deploy(ctx, c.tempMountPath,
			c.configMountPath+client.DefaultScriptName,
			c.cmdArgs, configmap.ObjectMeta.Name,
			c.configMountPath, c.requiredPorts)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestSpawn(t *testing.T) {
	ctx := context.Background()
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	cases := []struct {
		containerImage  string
		tempMountPath   string
		cmdArgs         []string
		configMountPath string
		configMapData   string
		resourceReq     apiv1.ResourceRequirements
	}{
		{
			containerImage:  "ubuntu:latest",
			tempMountPath:   "/wr-tmp/",
			cmdArgs:         []string{"tail", "-f", "/dev/null"},
			configMountPath: "/scripts/",
			configMapData:   "echo \"hello world\"",
			resourceReq: apiv1.ResourceRequirements{
				Requests: apiv1.ResourceList{
					apiv1.ResourceCPU:              *resource.NewMilliQuantity(int64(1)*1000, resource.DecimalSI),
					apiv1.ResourceMemory:           *resource.NewQuantity(int64(1)*1024*1024, resource.BinarySI),
					apiv1.ResourceEphemeralStorage: *resource.NewQuantity(int64(0)*1024*1024*1024, resource.BinarySI),
				},
				Limits: apiv1.ResourceList{
					apiv1.ResourceCPU:    *resource.NewMilliQuantity(int64(1)*1000, resource.DecimalSI),
					apiv1.ResourceMemory: *resource.NewQuantity(int64(1)*1024*1024, resource.BinarySI),
				},
			},
		},
	}

	for _, c := range cases {
		// Test the creation of config maps (2 birds one stone) We won't delete
		// this now so we can use it later.
		configmap, err := tc.CreateInitScriptConfigMap(c.configMapData)
		if err != nil {
			t.Error(err.Error())
		}

		expectedData := "#!/usr/bin/env bash\nset -euo pipefail\necho \"Running init script\"" +
			"\necho \"hello world\"\necho \"Init Script complete, executing arguments provided\"\nexec $@"

		if configmap.Data[client.DefaultScriptName] != expectedData {
			t.Error(fmt.Errorf("Unexpected contents of config map, got:\n%s \nexpect:\n%s", configmap.Data[client.DefaultScriptName], expectedData))
		}

		// create spawn request we run the init script created from wherever
		// we've decided to mount it.
		pod, err := tc.Spawn(ctx, c.containerImage, c.tempMountPath,
			c.configMountPath+client.DefaultScriptName,
			c.cmdArgs, configmap.ObjectMeta.Name,
			c.configMountPath, c.resourceReq)
		if err != nil {
			t.Error(err.Error())
		}

		// wait on the status to be running or pending
		err = wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
			pod, err = clientset.CoreV1().Pods(testingNamespace).Get(pod.ObjectMeta.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if pod.Status.Phase == "Pending" || pod.Status.Phase == "Running" {
				return true, nil
			}
			return false, nil
		})

		if err != nil {
			t.Error(fmt.Errorf("Spawn failed: %s", err))
		}

		// Now delete it
		err = tc.DestroyPod(ctx, pod.ObjectMeta.Name)
		if err != nil {
			t.Error(fmt.Errorf("Deleting pod failed: %s", err))
		}
	}
}

func TestInWrPod(t *testing.T) {
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}

	// These tests are not run inside a wr pod. Expect false
	inwr := client.InWRPod()
	if inwr {
		t.Error("This test is not run inside a pod wr controls, should return false.")
	}
}

func TestCreateInitScriptConfigMapFromFile(t *testing.T) {
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	cases := []struct {
		fileData string
		filePath string
	}{
		{
			fileData: "echo \"hello world\"",
			filePath: "/d1",
		},
	}

	dir, err := os.MkdirTemp("", "configMapFromFileTest")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	for _, c := range cases {
		err := os.WriteFile(dir+c.filePath, []byte(c.fileData), 0644)
		if err != nil {
			t.Error(fmt.Errorf("Failed to write file to %s: %s", dir+c.filePath, err))
		}
		cmap, err := tc.CreateInitScriptConfigMapFromFile(dir + c.filePath)
		if err != nil {
			t.Errorf("Failed to create config map: %s", err)
		}

		expectedData := "#!/usr/bin/env bash\nset -euo pipefail\necho \"Running init script\"" +
			"\necho \"hello world\"\necho \"Init Script complete, executing arguments provided\"\nexec $@"

		if cmap.Data[client.DefaultScriptName] != expectedData {
			t.Error(fmt.Errorf("Unexpected contents of config map, got:\n%s \nexpect:\n%s", cmap.Data[client.DefaultScriptName], expectedData))
		}
	}
}

// Must be called after deploy()
func TestTearDown(t *testing.T) {
	ctx := context.Background()
	if skip {
		t.Skip("skipping test; failed to access cluster")
	}
	cases := []struct {
		namespaceName string
	}{
		{
			namespaceName: testingNamespace,
		},
	}
	for _, c := range cases {
		err := tc.TearDown(ctx, c.namespaceName)
		if err != nil {
			t.Error(err.Error())
		}
		err = tc.TearDown(ctx, c.namespaceName)
		if err == nil {
			t.Error("TearDown should fail if called twice with the same input.")
		}
	}
}
