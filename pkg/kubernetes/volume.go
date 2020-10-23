/*
Copyright 2020 CyVerse
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubernetes

import (
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/iychoi/parcel-catalog-service/pkg/dataset"
	"github.com/lithammer/shortuuid/v3"
	apiv1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var (
	defaultStorageCapacity, _ = resourcev1.ParseQuantity("5Gi")
)

const (
	csiDriverName             = "parcel.csi.iychoi"
	csiDriverStorageClassName = "parcel-sc"

	// VolumeNamespace is a default namespace
	VolumeNamespace = "default"
)

// ParcelVolumeManager manages parcel volume
type ParcelVolumeManager struct {
	clientset *kubernetes.Clientset
	namespace string
}

// GetHomeKubernetesConfigPath returns a kubernetes configuration path under home
func GetHomeKubernetesConfigPath() (string, error) {
	home := homedir.HomeDir()

	if home != "" {
		return filepath.Join(home, ".kube", "config"), nil
	}
	return "", fmt.Errorf("cannot get home directory path")
}

// NewVolumeManager returns a new volume manager instance
func NewVolumeManager(configPath string, namespace string) (*ParcelVolumeManager, error) {
	config, err := clientcmd.BuildConfigFromFlags("", configPath)
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &ParcelVolumeManager{
		clientset: clientset,
		namespace: namespace,
	}, nil
}

// CreateStorageClass creates a new storage class
func (manager *ParcelVolumeManager) CreateStorageClass() error {
	sc, err := getStorageClass()
	if err != nil {
		return err
	}

	storageClient := manager.clientset.StorageV1()
	scList, err := storageClient.StorageClasses().List(metav1.ListOptions{})

	foundExisting := false
	if len(scList.Items) > 0 {
		for _, scExisting := range scList.Items {
			if scExisting.GetName() == sc.GetName() {
				foundExisting = true
				break
			}
		}
	}

	if !foundExisting {
		// create a new sc
		_, err := storageClient.StorageClasses().Create(sc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (manager *ParcelVolumeManager) CreateVolume(ds *dataset.Dataset) (*apiv1.PersistentVolume, *apiv1.PersistentVolumeClaim, error) {
	volumeName := getPersistentVolumeName(ds)
	pv, err := getPersistentVolume(ds, volumeName)
	if err != nil {
		return nil, nil, err
	}

	coreClient := manager.clientset.CoreV1()
	// create a new pv
	pvCreated, err := coreClient.PersistentVolumes().Create(pv)
	if err != nil {
		return nil, nil, err
	}

	pvc, err := getPersistentVolumeClaim(ds, volumeName)
	if err != nil {
		return nil, nil, err
	}

	pvcCreated, err := coreClient.PersistentVolumeClaims(manager.namespace).Create(pvc)
	if err != nil {
		return nil, nil, err
	}

	return pvCreated, pvcCreated, nil
}

func getClient(ds *dataset.Dataset) (string, error) {
	u, err := url.Parse(ds.URL)
	if err != nil {
		return "", fmt.Errorf("could not parse URL: %v", err)
	}

	scheme := strings.ToLower(u.Scheme)

	switch scheme {
	case "webdav":
		return "webdav", nil
	case "davfs":
		return "webdav", nil
	case "http":
		return "webdav", nil
	case "https":
		return "webdav", nil
	case "irods":
		return "irodsfuse", nil
	default:
		return "", fmt.Errorf("unknown scheme - %s", scheme)
	}
}

func getLabels(ds *dataset.Dataset, volumeName string) map[string]string {
	labels := map[string]string{
		"volume-name": volumeName,
	}
	return labels
}

func getPersistentVolumeName(ds *dataset.Dataset) string {
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		log.Fatal(err)
	}

	dsName := reg.ReplaceAllString(ds.Name, "")
	uuid := shortuuid.New()

	return fmt.Sprintf("parcel-pv-%s-%s", dsName, uuid)
}

func getPersistentVolumeClaimName(ds *dataset.Dataset, volumeName string) string {
	return fmt.Sprintf("%s-claim", volumeName)
}

func getPersistentVolumeHandleName(ds *dataset.Dataset, volumeName string) string {
	return fmt.Sprintf("%s-handle", volumeName)
}

func getStorageClass() (*storagev1.StorageClass, error) {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: csiDriverStorageClassName,
		},
		Provisioner: csiDriverName,
	}, nil
}

func getPersistentVolume(ds *dataset.Dataset, volumeName string) (*apiv1.PersistentVolume, error) {
	client, err := getClient(ds)
	if err != nil {
		return nil, err
	}

	labels := getLabels(ds, volumeName)
	volmode := apiv1.PersistentVolumeFilesystem
	return &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   volumeName,
			Labels: labels,
		},
		Spec: apiv1.PersistentVolumeSpec{
			Capacity: apiv1.ResourceList{
				apiv1.ResourceStorage: defaultStorageCapacity,
			},
			VolumeMode: &volmode,
			AccessModes: []apiv1.PersistentVolumeAccessMode{
				apiv1.ReadWriteMany,
			},
			//PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimDelete,
			PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimRetain,
			StorageClassName:              csiDriverStorageClassName,
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver:       csiDriverName,
					VolumeHandle: getPersistentVolumeHandleName(ds, volumeName),
					VolumeAttributes: map[string]string{
						"client": client,
						"url":    ds.URL,
						"user":   "anonymous",
					},
				},
			},
		},
	}, nil
}

func getPersistentVolumeClaim(ds *dataset.Dataset, volumeName string) (*apiv1.PersistentVolumeClaim, error) {
	labels := getLabels(ds, volumeName)
	storageclassname := csiDriverStorageClassName

	return &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   getPersistentVolumeClaimName(ds, volumeName),
			Labels: labels,
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			AccessModes: []apiv1.PersistentVolumeAccessMode{
				apiv1.ReadWriteMany,
			},
			StorageClassName: &storageclassname,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Resources: apiv1.ResourceRequirements{
				Requests: apiv1.ResourceList{
					apiv1.ResourceStorage: defaultStorageCapacity,
				},
			},
		},
	}, nil
}
