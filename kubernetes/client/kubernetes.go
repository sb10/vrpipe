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

package client

/*
Package client provides functions to interact with a kubernetes cluster, used to
create resources so that you can spawn runners, then delete those resources when
you're done. Everything is centred around a Kubernetesp struct. Calling
Authenticate() will allow AttachCmd and ExecCmd to work without needing to
Initialize().
*/

import (
	"context"
	"crypto/md5" // #nosec
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/wtsi-ssg/wr/clog"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	rbacapi "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	typedappsv1beta1 "k8s.io/client-go/kubernetes/typed/apps/v1beta1"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // Allow gcp auth.
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/retry"
)

// DefaultScriptName is the name to use as the default script in the create
// configmap functions.
const DefaultScriptName = "wr-boot"

// MinimalImage is the image to use when deploying wr manager and for init
// containers. It does not run user jobs. It needs tar, cat, bash,
// ca-certificates, fuse and other basic things.
const MinimalImage = "ubuntu:17.10"

// ScriptTop and ScriptBottom sandwich the user's script when creating a config
// map to boot from
const (
	ScriptTop    = "#!/usr/bin/env bash\nset -euo pipefail\necho \"Running init script\"\n"
	ScriptBottom = "\necho \"Init Script complete, executing arguments provided\"\nexec $@"
)

// Kubernetesp is the implementation for the kubernetes client library. It
// provides access to all methods defined in this package
type Kubernetesp struct {
	clientset         kubernetes.Interface
	clusterConfig     *rest.Config
	RESTClient        rest.Interface
	deploymentsClient typedappsv1beta1.DeploymentInterface
	serviceClient     typedv1.ServiceInterface
	podClient         typedv1.PodInterface
	configMapClient   typedv1.ConfigMapInterface
	configMapHashes   map[string][16]byte
	StopChannel       chan struct{}
	ReadyChannel      chan struct{}
	cmdOut, cmdErr    io.Writer
	NewNamespaceName  string
}

// ConfigMapOpts defines the name and Data (Binary, or strings) to store in a
// ConfigMap.
type ConfigMapOpts struct {
	// BinaryData for potential later use
	BinaryData []byte
	Data       map[string]string
	Name       string
}

// ServiceOpts defines basic options for a kubernetes service.
type ServiceOpts struct {
	Name      string
	Labels    map[string]string
	Selector  map[string]string
	ClusterIP string
	Ports     []apiv1.ServicePort
}

// AuthConfig holds configuration information necessary for using the client
// library.
type AuthConfig struct {
	KubeConfigPath string
}

// ConfigPath returns the set KubeConfigPath, or a default otherwise.
func (ac AuthConfig) ConfigPath() string {
	if len(ac.KubeConfigPath) == 0 {
		if kc := os.Getenv("KUBECONFIG"); kc != "" {
			return kc
		}

		if home := homedir.HomeDir(); home != "" {
			return filepath.Join(home, ".kube", "config")
		}
	}
	return ac.KubeConfigPath
}

func int32Ptr(i int32) *int32 { return &i }

func boolTrue() *bool {
	b := true
	return &b
}

// CreateNewNamespace Creates a new namespace with the provided name.
func (p *Kubernetesp) CreateNewNamespace(name string) error {
	_, nsErr := p.clientset.CoreV1().Namespaces().Create(&apiv1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	})
	return nsErr
}

// Authenticate with cluster, return clientset and RESTConfig. Can be called
// from within or outside of a cluster, should still work. Optionally supply a
// logger.
func (p *Kubernetesp) Authenticate(ctx context.Context, config AuthConfig) (kubernetes.Interface, *rest.Config, error) {
	kubeconfig := config.ConfigPath()

	// Determine if in cluster
	host, port, kubevar := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT"), os.Getenv("KUBECONFIG")

	switch {
	case (len(host) == 0 || len(port) == 0) && len(kubevar) == 0:
		clog.Debug(ctx, "getting authentication information", "path", kubeconfig)
		var err error
		clusterConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			clog.Error(ctx, "failed to build cluster configuration", "err", err, "path", kubeconfig)
			return nil, nil, fmt.Errorf("failed to build configuration from %s", kubeconfig)
		}

		// Create authenticated clientset
		clientset, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			clog.Error(ctx, "creating authenticated clientset", "err", err)
			return nil, nil, fmt.Errorf("failed to create authenticated clientset")
		}

		// Set up internal clientset and clusterConfig
		clog.Debug(ctx, "successfully read authentication information", "path", kubeconfig)
		p.clientset = clientset
		p.clusterConfig = clusterConfig

		// Create REST client
		p.RESTClient = clientset.CoreV1().RESTClient()
		return clientset, clusterConfig, nil
	case len(kubevar) != 0:
		clog.Debug(ctx, "authenticating using $KUBECONFIG", "path", kubevar)
		kubeconfig = kubevar

		clusterConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			clog.Error(ctx, "failed to build cluster configuration", "path", kubevar, "err", err)

		}

		// Create authenticated clientset
		clientset, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			clog.Error(ctx, "creating authenticated clientset", "err", err)
			return nil, nil, fmt.Errorf("failed to create authenticated clientset")
		}

		// Set up internal clientset and clusterConfig
		clog.Debug(ctx, "successfully read authentication information", "path", kubevar)
		p.clientset = clientset
		p.clusterConfig = clusterConfig
		// Create REST client
		p.RESTClient = clientset.CoreV1().RESTClient()
		return clientset, clusterConfig, nil
	default:
		clog.Debug(ctx, "authenticating using InClusterConfig()")
		clusterConfig, err := rest.InClusterConfig()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to authenticate in cluster")
		}

		// creates the clientset
		clientset, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create authenticated clientset")
		}

		// Set up internal clientset and clusterConfig
		p.clientset = clientset
		p.clusterConfig = clusterConfig

		// Create REST client
		p.RESTClient = clientset.CoreV1().RESTClient()

		clog.Debug(ctx, "successfully authenticated using InClusterConfig()")

		return clientset, clusterConfig, nil
	}
}

// Initialize uses the passed clientset to create some authenticated clients
// used in other methods. Creates a new namespace for wr to work in. Optionally
// pass a namespace as a string.
func (p *Kubernetesp) Initialize(ctx context.Context, clientset kubernetes.Interface, namespace ...string) error {
	// If a namespace is passed, check it exists. If it does not, create it. If
	// no namespace passed, create a random one.
	if len(namespace) == 1 {
		p.NewNamespaceName = namespace[0]
		_, err := clientset.CoreV1().Namespaces().Get(p.NewNamespaceName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				err = p.CreateNewNamespace(p.NewNamespaceName)
				if err != nil {
					clog.Crit(ctx, "failed to create provided namespace", "namespace", p.NewNamespaceName, "error", err)
					return err
				}
			} else {
				return err
			}
		}
	} else {
		rand.Seed(time.Now().UnixNano())
		p.NewNamespaceName = strings.Replace(namesgenerator.GetRandomName(0), "_", "-", -1) + "-wr"
		clog.Debug(ctx, "NewNamespaceName", p.NewNamespaceName)
		// Retry if namespace taken
		retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			nsErr := p.CreateNewNamespace(p.NewNamespaceName)
			if nsErr != nil {
				clog.Warn(ctx, "failed to create new namespace. Trying again.", "namespace", p.NewNamespaceName, "err", nsErr)
				p.NewNamespaceName = strings.Replace(namesgenerator.GetRandomName(0), "_", "-", -1) + "-wr"
			}
			return nsErr
		})
		if retryErr != nil {
			clog.Error(ctx, "creation of new namespace failed", "err", retryErr)
			return fmt.Errorf("creation of new namespace failed: %v", retryErr)
		}
	}

	// Create client for deployments that is authenticated against the given
	// cluster. Use default namsespace.
	p.deploymentsClient = clientset.AppsV1beta1().Deployments(p.NewNamespaceName)

	// Create client for services
	p.serviceClient = clientset.CoreV1().Services(p.NewNamespaceName)

	// Create client for pods
	p.podClient = clientset.CoreV1().Pods(p.NewNamespaceName)

	// Create configMap client
	p.configMapClient = clientset.CoreV1().ConfigMaps(p.NewNamespaceName)

	// Store md5 hashes of the data in each configMap. used when calling
	// newConfigMap().
	p.configMapHashes = map[string][16]byte{}

	// ToDO: This assumes one portforward per namespaced deployment This should
	// probably go in pod and the channels be created in an options struct to
	// better isolate the logic

	// Make channels for port forwarding
	p.StopChannel = make(chan struct{}, 1)
	p.ReadyChannel = make(chan struct{})

	return nil
}

// Deploy creates the wr-manager deployment and service. It creates a
// ClusterRoleBinding to allow the default service account in the namespace
// rights to manage the cluster. (ToDo: Write own ClusterRole  / Role) This
// allows copying of wr to initcontainer, done by controller when ready (assumes
// tar is available). Portforwarding done by controller when ready.
// TempMountPath is the path at which the 'wr-tmp' directory is set to. It is
// also set to $HOME. Command is the command to be executed in the container.
// CmdArgs are the arguments to pass to the supplied command. ConfigMapName is
// the name of the configmap to mount at the configMountPath provided.
func (p *Kubernetesp) Deploy(ctx context.Context, tempMountPath string, command string, cmdArgs []string, configMapName string, configMountPath string, requiredPorts []int) error {
	// Patch the default cluster role to allow pods and nodes to be viewed.
	_, err := p.clientset.RbacV1().ClusterRoleBindings().Create(&rbacapi.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "wr-cluster-role-binding-",
			Labels:       map[string]string{"wr-" + p.NewNamespaceName: "ClusterRoleBinding"},
		},
		Subjects: []rbacapi.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "default",
				Namespace: p.NewNamespaceName,
			},
		},
		RoleRef: rbacapi.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create cluster role binding in namespace %s: %s", p.NewNamespaceName, err)
	}

	// Specify new wr deployment
	deployment := &appsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "wr-manager",
		},
		Spec: appsv1beta1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: "wr-manager",
					Labels: map[string]string{
						"app": "wr-manager",
					},
				},
				Spec: apiv1.PodSpec{
					Volumes: []apiv1.Volume{
						{
							Name: "wr-temp",
							VolumeSource: apiv1.VolumeSource{
								EmptyDir: &apiv1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: configMapName,
							VolumeSource: apiv1.VolumeSource{
								ConfigMap: &apiv1.ConfigMapVolumeSource{
									LocalObjectReference: apiv1.LocalObjectReference{
										Name: configMapName,
									},
									DefaultMode: int32Ptr(0777),
								},
							},
						},
					},
					Containers: []apiv1.Container{
						{
							Name:  "wr-manager",
							Image: MinimalImage,
							Ports: []apiv1.ContainerPort{
								{
									Name:          "wr-manager",
									Protocol:      apiv1.ProtocolTCP,
									ContainerPort: int32(requiredPorts[0]),
								},
								{
									Name:          "wr-web",
									Protocol:      apiv1.ProtocolTCP,
									ContainerPort: int32(requiredPorts[1]),
								},
							},
							Command: []string{command},
							Args:    cmdArgs,
							VolumeMounts: []apiv1.VolumeMount{
								{
									Name:      "wr-temp",
									MountPath: tempMountPath,
								},
								{
									Name:      configMapName,
									MountPath: configMountPath,
								},
							},
							Env: []apiv1.EnvVar{
								{
									Name:  "WR_MANAGERPORT",
									Value: strconv.Itoa(requiredPorts[0]),
								},
								{
									Name:  "WR_MANAGERWEB",
									Value: strconv.Itoa(requiredPorts[1]),
								},
								{
									Name:  "HOME",
									Value: tempMountPath,
								},
								{
									Name:  "USER",
									Value: "root",
								},
							},
							SecurityContext: &apiv1.SecurityContext{
								Privileged: boolTrue(),
							},
						},
					},
					InitContainers: []apiv1.Container{
						{
							Name:      "init-container",
							Image:     MinimalImage,
							Command:   []string{"/bin/tar", "-xf", "-"},
							Stdin:     true,
							StdinOnce: true,
							VolumeMounts: []apiv1.VolumeMount{
								{
									Name:      "wr-temp",
									MountPath: tempMountPath,
								},
							},
						},
					},
					Hostname: "wr-manager",
				},
			},
		},
	}

	// Create Deployment
	clog.Debug(ctx, "Creating deployment...")
	result, err := p.deploymentsClient.Create(deployment)
	if err != nil {
		clog.Error(ctx, "creating deployment", "err", err)
		return err
	}
	clog.Debug(ctx, "Created deployment", "name", result.GetObjectMeta().GetName(), "namespace", p.NewNamespaceName)
	svcOpts := ServiceOpts{
		Name: "wr-manager",
		Labels: map[string]string{
			"app": "wr-manager",
		},
		Selector: map[string]string{
			"app": "wr-manager",
		},
		ClusterIP: "None",
		Ports: []apiv1.ServicePort{
			{
				Name: "wr-manager",
				Port: int32(requiredPorts[0]),
				TargetPort: intstr.IntOrString{
					IntVal: int32(requiredPorts[0]),
				},
			},
		},
	}

	err = p.CreateService(&svcOpts)
	if err != nil {
		clog.Error(ctx, "creating service", "err", err)
		return err
	}
	return nil
}

// InWRPod checks if we are in a wr pod. As we control the hostname, just check
// if the hostname contains 'wr-' in addition to the standard environment
// variables.
func InWRPod() bool {
	hostname, err := os.Hostname()
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	inPod := false
	if err == nil && len(host) != 0 && len(port) != 0 {
		if strings.Contains(hostname, "wr-") {
			inPod = true
		}
	}
	return inPod
}

// Spawn a new pod that expects an 'attach' command to tar some files across
// (Init container). It also names the pod 'wr-runner-xxxx' and mounts the
// provided config map at the path provided. The directory named 'wr-tmp'
// persists between the two containers, so any files that you want to survive
// the tar step should untar to this path only. This path is also set as $HOME.
// This path is set with the tempMountPath variable.
func (p *Kubernetesp) Spawn(ctx context.Context, baseContainerImage string, tempMountPath string, binaryPath string, binaryArgs []string, configMapName string, configMountPath string, resources apiv1.ResourceRequirements) (*apiv1.Pod, error) {
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "wr-runner-",
			Labels: map[string]string{
				"wr": "runner",
			},
		},
		Spec: apiv1.PodSpec{
			RestartPolicy: apiv1.RestartPolicyNever,
			Volumes: []apiv1.Volume{
				{
					Name: "wr-temp",
					VolumeSource: apiv1.VolumeSource{
						EmptyDir: &apiv1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: configMapName,
					VolumeSource: apiv1.VolumeSource{
						ConfigMap: &apiv1.ConfigMapVolumeSource{
							LocalObjectReference: apiv1.LocalObjectReference{
								Name: configMapName,
							},
							DefaultMode: int32Ptr(0777),
						},
					},
				},
			},
			Containers: []apiv1.Container{
				{
					Name:    "wr-runner",
					Image:   baseContainerImage,
					Command: []string{binaryPath},
					Args:    binaryArgs,
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "wr-temp",
							MountPath: tempMountPath,
						},
						{
							Name:      configMapName,
							MountPath: configMountPath,
						},
					},
					Env: []apiv1.EnvVar{
						{
							Name:  "HOME",
							Value: tempMountPath,
						},
						{
							Name:  "USER",
							Value: "root",
						},
					},
					Resources: resources,
					SecurityContext: &apiv1.SecurityContext{
						Privileged: boolTrue(),
						Capabilities: &apiv1.Capabilities{
							Add: []apiv1.Capability{"SYS_ADMIN"},
						},
					},
				},
			},
			InitContainers: []apiv1.Container{
				{
					Name:      "runner-init-container",
					Image:     MinimalImage,
					Command:   []string{"/bin/tar", "-xf", "-"},
					Stdin:     true,
					StdinOnce: true,
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "wr-temp",
							MountPath: tempMountPath,
						},
					},
				},
			},
		},
	}

	// Create pod
	pod, err := p.podClient.Create(pod)
	if err != nil {
		clog.Error(ctx, "failed to create pod", "err", err)
		return nil, err
	}

	return pod, err
}

// TearDown deletes the namespace and cluster role binding created for wr.
func (p *Kubernetesp) TearDown(ctx context.Context, namespace string) error {
	err := p.clientset.CoreV1().Namespaces().Delete(namespace, &metav1.DeleteOptions{})
	if err != nil {
		clog.Error(ctx, "deleting namespace", "err", err, "namespace", namespace)
		return err
	}

	crbl, err := p.clientset.RbacV1().ClusterRoleBindings().List(metav1.ListOptions{
		LabelSelector: "wr-" + namespace,
	})
	if err != nil {
		clog.Error(ctx, "getting ClusterRoleBindings", "err", err)
		return err
	}
	err = p.clientset.RbacV1().ClusterRoleBindings().Delete(crbl.Items[0].ObjectMeta.Name, &metav1.DeleteOptions{})
	if err != nil {
		clog.Error(ctx, "deleting ClusterRoleBinding", "ClusterRoleBinding", crbl.Items[0].ObjectMeta.Name, "err", err)
		return err
	}

	return nil
}

// DestroyPod deletes the given pod, doesn't check it exists first.
func (p *Kubernetesp) DestroyPod(ctx context.Context, podName string) error {
	err := p.podClient.Delete(podName, &metav1.DeleteOptions{})
	if err != nil {
		clog.Error(ctx, "deleting pod", "err", err, "pod", podName)
		return err
	}
	return nil
}

// NewConfigMap creates a new configMap. It checks the contents are not
// identical to a previously created config map by comparing hashes of the data.
// Kubernetes 1.10 (Released Late March 2018) provides a BinaryData field that
// could be used to replace the initContainer method for copying the executable.
// At the moment is not appropriate as it's not likely most users are running
// 1.10. It's a good idea once enough users are using 1.10+ as it simplifies the
// copy step considerably, and removes much complexity.
func (p *Kubernetesp) NewConfigMap(opts *ConfigMapOpts) (*apiv1.ConfigMap, error) {
	//Check if we have already created a config map with a script with the same
	//hash.

	// Calculate hash of opts.Data, json stringify it first.
	jsonData, err := json.Marshal(opts.Data)
	if err != nil {
		return nil, err
	}
	md5 := md5.Sum(jsonData) //#nosec

	var match string
	for k, v := range p.configMapHashes {
		if v == md5 {
			match = k
		}
	}
	if len(match) != 0 {
		return p.configMapClient.Get(match, metav1.GetOptions{})
	}

	// No configmap with the data we want exists, so create one.
	configMap, err := p.configMapClient.Create(&apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "wr-script-",
		},
		Data: opts.Data,
	})

	// Store the name and md5 in configMapHashes
	p.configMapHashes[configMap.ObjectMeta.Name] = md5

	return configMap, err
}

// CreateInitScriptConfigMapFromFile is the same as CreateInitScriptConfigMap
// but takes a path to a file as input.
func (p *Kubernetesp) CreateInitScriptConfigMapFromFile(scriptPath string) (*apiv1.ConfigMap, error) {
	// read in the script given
	buf, err := os.ReadFile(scriptPath)
	if err != nil {
		return nil, err
	}

	// Insert script into template
	return p.NewConfigMap(&ConfigMapOpts{
		Data: map[string]string{DefaultScriptName: ScriptTop + string(buf) + ScriptBottom},
	})
}

// CreateInitScriptConfigMap performs very basic string fudging. This allows a
// wr pod to execute some arbitrary script before starting the runner / manager.
// So far it appears to work.
func (p *Kubernetesp) CreateInitScriptConfigMap(script string) (*apiv1.ConfigMap, error) {
	return p.NewConfigMap(&ConfigMapOpts{
		Data: map[string]string{DefaultScriptName: ScriptTop + script + ScriptBottom},
	})
}

// CreateService Creates a service with the defined options from ServiceOpts.
func (p *Kubernetesp) CreateService(opts *ServiceOpts) error {
	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   opts.Name,
			Labels: opts.Labels,
		},
		Spec: apiv1.ServiceSpec{
			Selector:  opts.Selector,
			ClusterIP: opts.ClusterIP,
			Ports:     opts.Ports,
		},
	}

	_, err := p.serviceClient.Create(service)
	return err
}

// CheckWRDeploymentExists checks  if a wr deployment exists in a given namespace. If
// the deployment exists, it returns true. If no deployment is found it returns
// false and silences the not found error. If the deployment exists but an error is
// returned it returns true with the error.
func (p *Kubernetesp) CheckWRDeploymentExists(namespace string) (bool, error) {
	_, err := p.clientset.AppsV1beta1().Deployments(namespace).Get("wr-manager", metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// The deployment was not found, therefore it is not healthy.
			return false, nil
		}
	}
	// Some error may, the deployment exists. Return true and the possible error
	return true, err
}

// CheckWRDeploymentHealthy checks if the wr deployment in a given namespace is
// healthy. It does this by getting the deployment object for 'wr-manager' and
// checking the DeploymentAvailable condition is true.
func (p *Kubernetesp) CheckWRDeploymentHealthy(namespace string) (bool, error) {
	deployment, err := p.clientset.AppsV1beta1().Deployments(namespace).Get("wr-manager", metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	// We've successfully got the deployment, lets check the Available condition
	// and make sure its active. If it's not active, it may be in the
	// CrashLoopBackOff cycle.
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1beta1.DeploymentAvailable {
			if condition.Status == apiv1.ConditionFalse {
				return false, fmt.Errorf("deployment unhealthy: %s", condition.Message)
			}
		}
	}
	return true, nil

}
