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
	"strconv"
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

// DatasetMount holds a volume mapping
type DatasetMount struct {
	Dataset               *dataset.Dataset
	PersistentVolume      *apiv1.PersistentVolume
	PersistentVolumeClaim *apiv1.PersistentVolumeClaim
}

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
	sc, err := makeStorageClass()
	if err != nil {
		return err
	}

	storageClient := manager.clientset.StorageV1()
	scList, err := storageClient.StorageClasses().List(metav1.ListOptions{})

	foundExisting := false
	for _, scExisting := range scList.Items {
		if scExisting.GetName() == sc.GetName() {
			foundExisting = true
			break
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

// CreateVolume creates a Persistent Volume for Kubernetes
func (manager *ParcelVolumeManager) CreateVolume(ds *dataset.Dataset) (*DatasetMount, error) {
	volumeName := makePersistentVolumeName(ds)
	pv, err := makePersistentVolume(ds, volumeName)
	if err != nil {
		return nil, err
	}

	coreClient := manager.clientset.CoreV1()
	// create a new pv
	pvCreated, err := coreClient.PersistentVolumes().Create(pv)
	if err != nil {
		return nil, err
	}

	pvc, err := makePersistentVolumeClaim(ds, volumeName)
	if err != nil {
		return nil, err
	}

	pvcCreated, err := coreClient.PersistentVolumeClaims(manager.namespace).Create(pvc)
	if err != nil {
		return nil, err
	}

	return &DatasetMount{
		Dataset:               ds,
		PersistentVolume:      pvCreated,
		PersistentVolumeClaim: pvcCreated,
	}, nil
}

// ListVolumes lists Persistent Volumes for Kubernetes
func (manager *ParcelVolumeManager) ListVolumes() ([]*DatasetMount, error) {
	coreClient := manager.clientset.CoreV1()
	// list pv
	pvList, err := coreClient.PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	pvcList, err := coreClient.PersistentVolumeClaims(manager.namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	mounts := []*DatasetMount{}

	for _, pv := range pvList.Items {

		dataset := dataset.Dataset{}
		if checkPersistentVolumeName(&pv) {
			datasetID, found := pv.Labels["dataset-id"]
			if !found {
				continue
			}

			dataset.ID, err = strconv.ParseInt(datasetID, 10, 64)
			if err != nil {
				continue
			}

			datasetName, found := pv.Labels["dataset-name"]
			if !found {
				continue
			}

			dataset.Name = datasetName

			// get pvc
			for _, pvc := range pvcList.Items {
				if pv.Name == pvc.Labels["volume-name"] {
					mount := DatasetMount{
						Dataset:               &dataset,
						PersistentVolume:      &pv,
						PersistentVolumeClaim: &pvc,
					}

					mounts = append(mounts, &mount)
					break
				}
			}
		}
	}

	return mounts, nil
}

// GetVolume returns a Persistent Volume for Kubernetes
func (manager *ParcelVolumeManager) GetVolume(volumeName string) (*DatasetMount, error) {
	coreClient := manager.clientset.CoreV1()
	// get pv
	pv, err := coreClient.PersistentVolumes().Get(volumeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if pv.Name != volumeName {
		return nil, fmt.Errorf("Could not find pv with name %s", volumeName)
	}

	pvc, err := coreClient.PersistentVolumeClaims(manager.namespace).Get(makePersistentVolumeClaimName(volumeName), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if pvc.Labels["volume-name"] != volumeName {
		return nil, fmt.Errorf("Could not find pvc with name %s", volumeName)
	}

	dataset := dataset.Dataset{}
	datasetID, found := pv.Labels["dataset-id"]
	if !found {
		return nil, fmt.Errorf("Could not find 'dataset-id' field in a persistent volume")
	}

	dataset.ID, err = strconv.ParseInt(datasetID, 10, 64)
	if err != nil {
		return nil, err
	}

	datasetName, found := pv.Labels["dataset-name"]
	if !found {
		return nil, fmt.Errorf("Could not find 'dataset-name' field in a persistent volume")
	}

	dataset.Name = datasetName

	return &DatasetMount{
		Dataset:               &dataset,
		PersistentVolume:      pv,
		PersistentVolumeClaim: pvc,
	}, nil
}

// DeleteVolume deletes a Persistent Volume for Kubernetes
func (manager *ParcelVolumeManager) DeleteVolume(volumeName string) error {
	coreClient := manager.clientset.CoreV1()

	// delete pvc
	err := coreClient.PersistentVolumeClaims(manager.namespace).Delete(makePersistentVolumeClaimName(volumeName), &metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	// delete pv
	err = coreClient.PersistentVolumes().Delete(volumeName, &metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return nil
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

func makeLabels(ds *dataset.Dataset, volumeName string) map[string]string {
	labels := map[string]string{
		"volume-name":  volumeName,
		"dataset-id":   strconv.FormatInt(ds.ID, 10),
		"dataset-name": ds.Name,
	}
	return labels
}

func checkPersistentVolumeName(pv *apiv1.PersistentVolume) bool {
	return strings.HasPrefix(pv.Name, "parcel-pv-")
}

func makePersistentVolumeName(ds *dataset.Dataset) string {
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		log.Fatal(err)
	}

	dsName := reg.ReplaceAllString(ds.Name, "")
	uuid := shortuuid.New()

	return fmt.Sprintf("parcel-pv-%s-%s", dsName, uuid)
}

func makePersistentVolumeClaimName(volumeName string) string {
	return fmt.Sprintf("%s-claim", volumeName)
}

func makePersistentVolumeHandleName(volumeName string) string {
	return fmt.Sprintf("%s-handle", volumeName)
}

func makeStorageClass() (*storagev1.StorageClass, error) {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: csiDriverStorageClassName,
		},
		Provisioner: csiDriverName,
	}, nil
}

func makePersistentVolume(ds *dataset.Dataset, volumeName string) (*apiv1.PersistentVolume, error) {
	client, err := getClient(ds)
	if err != nil {
		return nil, err
	}

	labels := makeLabels(ds, volumeName)
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
					VolumeHandle: makePersistentVolumeHandleName(volumeName),
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

func makePersistentVolumeClaim(ds *dataset.Dataset, volumeName string) (*apiv1.PersistentVolumeClaim, error) {
	labels := makeLabels(ds, volumeName)
	storageclassname := csiDriverStorageClassName

	return &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   makePersistentVolumeClaimName(volumeName),
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
