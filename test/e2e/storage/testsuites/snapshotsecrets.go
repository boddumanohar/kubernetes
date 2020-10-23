/*
Copyright 2018 The Kubernetes Authors.

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

package testsuites

import (
	"context"
	"fmt"

	"time"

	"github.com/onsi/ginkgo"
	"k8s.io/client-go/dynamic"
	v1 "k8s.io/api/core/v1"
	// storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	// utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"

	// e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	"k8s.io/kubernetes/test/e2e/storage/testpatterns"
	"k8s.io/kubernetes/test/e2e/storage/utils"
)

type snapshotsecretsTestSuite struct {
	tsInfo TestSuiteInfo
}

var _ TestSuite = &snapshotsecretsTestSuite{}

// InitSnapshotsecretsTestSuite returns snapshottableTestSuite that implements TestSuite interface
func InitSnapshotsecretsTestSuite() TestSuite {
	return &snapshotsecretsTestSuite{
		tsInfo: TestSuiteInfo{
			Name: "snapshotsecrets",
			TestPatterns: []testpatterns.TestPattern{
				// testpatterns.DynamicSnapshotDelete,
				testpatterns.DynamicSnapshotRetain,
				// testpatterns.PreprovisionedSnapshotDelete,
				// testpatterns.PreprovisionedSnapshotRetain,
			},
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
			FeatureTag: "[Feature:VolumeSnapshotDataSource]",
		},
	}
}

func (s *snapshotsecretsTestSuite) GetTestSuiteInfo() TestSuiteInfo {
	return s.tsInfo
}

func (s *snapshotsecretsTestSuite) SkipRedundantSuite(driver TestDriver, pattern testpatterns.TestPattern) {
}

func (s *snapshotsecretsTestSuite) DefineTests(driver TestDriver, pattern testpatterns.TestPattern) {
	ginkgo.BeforeEach(func() {
		// Check preconditions.
		dInfo := driver.GetDriverInfo()
		ok := false
		sDriver, ok = driver.(SnapshottableTestDriver)
		if !dInfo.Capabilities[CapSnapshotDataSource] || !ok {
			e2eskipper.Skipf("Driver %q does not support snapshots - skipping", dInfo.Name)
		}
		dDriver, ok = driver.(DynamicPVTestDriver)
		if !ok {
			e2eskipper.Skipf("Driver %q does not support dynamic provisioning - skipping", driver.GetDriverInfo().Name)
		}
	})

	// This intentionally comes after checking the preconditions because it
	// registers its own BeforeEach which creates the namespace. Beware that it
	// also registers an AfterEach which renders f unusable. Any code using
	// f must run inside an It or Context callback.
	f := framework.NewDefaultFramework("snapshottingwithsecrets")

	ginkgo.Describe("volume snapshot controller", func() {
		var (
			err           error
			config        *PerTestConfig
			driverCleanup func()
			cleanupSteps  []func()

			cs clientset.Interface
			dc                  dynamic.Interface
			pvc                 *v1.PersistentVolumeClaim
			// sc                  *storagev1.StorageClass
			// claimSize           string
			// originalMntTestData string
		)
		init := func() {
			cleanupSteps = make([]func(), 0)
			// init snap class, create a source PV, PVC, Pod
			cs = f.ClientSet
			dc = f.DynamicClient

			// Now do the more expensive test initialization.
			config, driverCleanup = driver.PrepareTest(f)
			cleanupSteps = append(cleanupSteps, driverCleanup)

			var volumeResource *VolumeResource
			cleanupSteps = append(cleanupSteps, func() {
				framework.ExpectNoError(volumeResource.CleanupResource())
			})

			ginkgo.By("Create volume resource using secrets")
			volumeResource = CreateVolumeResource(dDriver, config, pattern, s.GetTestSuiteInfo().SupportedSizeRange)

			pvc = volumeResource.Pvc
			// sc = volumeResource.Sc
			// claimSize = pvc.Spec.Resources.Requests.Storage().String()

			ginkgo.By("starting a pod to use the claim")

			err = e2epv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, cs, pvc.Namespace, pvc.Name, framework.Poll, framework.ClaimProvisionTimeout)
			framework.ExpectNoError(err)

			ginkgo.By("checking the claim")
			// Get new copy of the claim
			pvc, err = cs.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			// Get the bound PV
			ginkgo.By("checking the PV")
			_, err = cs.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
			framework.ExpectNoError(err)

		}

		cleanup := func() {
			// Don't register an AfterEach then a cleanup step because the order
			// of execution will do the AfterEach first then the cleanup step.
			// Also AfterEach cleanup registration is not fine grained enough
			// Adding to the cleanup steps allows you to register cleanup only when it is needed
			// Ideally we could replace this with https://golang.org/pkg/testing/#T.Cleanup

			// Depending on how far the test executed, cleanup accordingly
			// Execute in reverse order, similar to defer stack
			for i := len(cleanupSteps) - 1; i >= 0; i-- {
				err := tryFunc(cleanupSteps[i])
				framework.ExpectNoError(err, "while running cleanup steps")
			}

		}
		ginkgo.BeforeEach(func() {
			init()
		})
		ginkgo.AfterEach(func() {
			cleanup()
		})

		ginkgo.Context("", func() {
			var (
				vs        *unstructured.Unstructured
				// vscontent *unstructured.Unstructured
				// vsc       *unstructured.Unstructured
				secret    *v1.Secret
			)

			ginkgo.BeforeEach(func() {
				var sr *SnapshotResource
				cleanupSteps = append(cleanupSteps, func() {
					framework.ExpectNoError(sr.CleanupResource())
				})
				// create a secret
				secret = &v1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: f.Namespace.Name,
						Name:      "snapshot-secret",
					},
					Data: map[string][]byte{
						"secret-data": []byte("secret-value-1"),
					},
				}

				if secret, err = cs.CoreV1().Secrets(f.Namespace.Name).Create(context.TODO(), secret, metav1.CreateOptions{}); err != nil {
					framework.Failf("unable to create test secret %s: %v", secret.Name, err)
				}

				// Create snapshot using secret
				sr = CreateSnapshotResourceWithSecrets(sDriver, config, pattern, pvc.GetName(), pvc.GetNamespace())
				vs = sr.Vs
				// vscontent = sr.Vscontent
				// vsc = sr.Vsclass
			})
			ginkgo.It("Use secrets field, create/list and delete snapshots", func() {

				ginkgo.By("list the snapshot")
				// TODO: Check if list snapshot is supported. Log and continue if not supported.
				ListSnapshotsWithSecrets(dc, vs.GetNamespace(), vs.GetName(), framework.Poll, framework.SnapshotDeleteTimeout)
				// Snapshots should be allowed to be deleted even if secrets are deleted.
				if secret != nil {
					framework.Logf("Deleting the secret %q...", secret.Name)
					err := cs.CoreV1().Secrets(f.Namespace.Name).Delete(context.TODO(), secret.Name, metav1.DeleteOptions{})
					if err != nil {
						framework.Logf("Delete secret failed: %v", err)
					}
				}
				ginkgo.By("Delete the snapshot")
				DeleteSnapshotWithSecrets(dc, vs.GetNamespace(), vs.GetName(), framework.Poll, framework.SnapshotDeleteTimeout)
				
				// ginkgo.By("checking if the snapshot is deleted")
				// 
			})
		})
	})
}

func ListSnapshotsWithSecrets(dc dynamic.Interface, ns string, snapshotName string, poll, timeout time.Duration) error {
	ginkgo.By("deleting the snapshot")
	err := dc.Resource(SnapshotGVR).Namespace(ns).Delete(context.TODO(), snapshotName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// DeleteSnapshotWithSecrets deletes a VolumeSnapshot and waits for it to be deleted or until timeout occurs, whichever comes first
func DeleteSnapshotWithSecrets(dc dynamic.Interface, ns string, snapshotName string, poll, timeout time.Duration) error {
	var err error
	ginkgo.By("deleting the snapshot")
	err = dc.Resource(SnapshotGVR).Namespace(ns).Delete(context.TODO(), snapshotName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	ginkgo.By("checking the Snapshot has been deleted")
	err = utils.WaitForNamespacedGVRDeletion(dc, SnapshotGVR, ns, snapshotName, poll, timeout)

	return err
}

func CreateSnapshotResourceWithSecrets(sDriver SnapshottableTestDriver, config *PerTestConfig, pattern testpatterns.TestPattern, pvcName string, pvcNamespace string) *SnapshotResource {

	var err error
	if pattern.SnapshotType != testpatterns.DynamicCreatedSnapshot && pattern.SnapshotType != testpatterns.PreprovisionedCreatedSnapshot {
		err = fmt.Errorf("SnapshotType must be set to either DynamicCreatedSnapshot or PreprovisionedCreatedSnapshot")
		framework.ExpectNoError(err)
	}
	r := SnapshotResource{
		Config:  config,
		Pattern: pattern,
	}
	dc := r.Config.Framework.DynamicClient

	ginkgo.By("creating a SnapshotClass")
	parameters := map[string]string{
		"csi.storage.k8s.io/snapshotter-secret-name": "snapshot-secret",
		"csi.storage.k8s.io/snapshotter-secret-namespace": pvcNamespace,
	}
	r.Vsclass = sDriver.GetSnapshotClass(config, parameters)
	if r.Vsclass == nil {
		framework.Failf("Failed to get snapshot class based on test config")
	}
	r.Vsclass.Object["deletionPolicy"] = pattern.SnapshotDeletionPolicy.String()

	r.Vsclass, err = dc.Resource(SnapshotClassGVR).Create(context.TODO(), r.Vsclass, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	r.Vsclass, err = dc.Resource(SnapshotClassGVR).Get(context.TODO(), r.Vsclass.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err)

	ginkgo.By("creating a dynamic VolumeSnapshot")
	// prepare a dynamically provisioned volume snapshot with certain data
	r.Vs = getSnapshot(pvcName, pvcNamespace, r.Vsclass.GetName())

	r.Vs, err = dc.Resource(SnapshotGVR).Namespace(r.Vs.GetNamespace()).Create(context.TODO(), r.Vs, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	err = WaitForSnapshotReady(dc, r.Vs.GetNamespace(), r.Vs.GetName(), framework.Poll, framework.SnapshotCreateTimeout)
	framework.ExpectNoError(err)

	r.Vs, err = dc.Resource(SnapshotGVR).Namespace(r.Vs.GetNamespace()).Get(context.TODO(), r.Vs.GetName(), metav1.GetOptions{})

	snapshotStatus := r.Vs.Object["status"].(map[string]interface{})
	snapshotContentName := snapshotStatus["boundVolumeSnapshotContentName"].(string)
	framework.Logf("received snapshotStatus %v", snapshotStatus)
	framework.Logf("snapshotContentName %s", snapshotContentName)
	framework.ExpectNoError(err)

	r.Vscontent, err = dc.Resource(SnapshotContentGVR).Get(context.TODO(), snapshotContentName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	if pattern.SnapshotType == testpatterns.PreprovisionedCreatedSnapshot {
		// prepare a pre-provisioned VolumeSnapshotContent with certain data
		// Because this could be run with an external CSI driver, we have no way
		// to pre-provision the snapshot as we normally would using their API.
		// We instead dynamically take a snapshot (above step), delete the old snapshot,
		// and create another snapshot using the first snapshot's snapshot handle.

		ginkgo.By("updating the snapshot content deletion policy to retain")
		r.Vscontent.Object["spec"].(map[string]interface{})["deletionPolicy"] = "Retain"

		r.Vscontent, err = dc.Resource(SnapshotContentGVR).Update(context.TODO(), r.Vscontent, metav1.UpdateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("recording the volume handle and snapshotHandle")
		snapshotHandle := r.Vscontent.Object["status"].(map[string]interface{})["snapshotHandle"].(string)
		framework.Logf("Recording snapshot handle: %s", snapshotHandle)
		csiDriverName := r.Vsclass.Object["driver"].(string)

		// If the deletion policy is retain on vscontent:
		// when vs is deleted vscontent will not be deleted
		// when the vscontent is manually deleted then the underlying snapshot resource will not be deleted.
		// We exploit this to create a snapshot resource from which we can create a preprovisioned snapshot
		ginkgo.By("deleting the snapshot and snapshot content")
		err = dc.Resource(SnapshotGVR).Namespace(r.Vs.GetNamespace()).Delete(context.TODO(), r.Vs.GetName(), metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			err = nil
		}
		framework.ExpectNoError(err)

		ginkgo.By("checking the Snapshot has been deleted")
		err = utils.WaitForNamespacedGVRDeletion(dc, SnapshotGVR, r.Vs.GetName(), r.Vs.GetNamespace(), framework.Poll, framework.SnapshotDeleteTimeout)
		framework.ExpectNoError(err)

		err = dc.Resource(SnapshotContentGVR).Delete(context.TODO(), r.Vscontent.GetName(), metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			err = nil
		}
		framework.ExpectNoError(err)

		ginkgo.By("checking the Snapshot content has been deleted")
		err = utils.WaitForGVRDeletion(dc, SnapshotContentGVR, r.Vscontent.GetName(), framework.Poll, framework.SnapshotDeleteTimeout)
		framework.ExpectNoError(err)

		ginkgo.By("creating a snapshot content with the snapshot handle")
		uuid := uuid.NewUUID()

		snapName := getPreProvisionedSnapshotName(uuid)
		snapcontentName := getPreProvisionedSnapshotContentName(uuid)

		r.Vscontent = getPreProvisionedSnapshotContent(snapcontentName, snapName, pvcNamespace, snapshotHandle, pattern.SnapshotDeletionPolicy.String(), csiDriverName)
		r.Vscontent, err = dc.Resource(SnapshotContentGVR).Create(context.TODO(), r.Vscontent, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("creating a snapshot with that snapshot content")
		r.Vs = getPreProvisionedSnapshot(snapName, pvcNamespace, snapcontentName)
		r.Vs, err = dc.Resource(SnapshotGVR).Namespace(r.Vs.GetNamespace()).Create(context.TODO(), r.Vs, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		err = WaitForSnapshotReady(dc, r.Vs.GetNamespace(), r.Vs.GetName(), framework.Poll, framework.SnapshotCreateTimeout)
		framework.ExpectNoError(err)

		ginkgo.By("getting the snapshot and snapshot content")
		r.Vs, err = dc.Resource(SnapshotGVR).Namespace(r.Vs.GetNamespace()).Get(context.TODO(), r.Vs.GetName(), metav1.GetOptions{})
		framework.ExpectNoError(err)

		r.Vscontent, err = dc.Resource(SnapshotContentGVR).Get(context.TODO(), r.Vscontent.GetName(), metav1.GetOptions{})
		framework.ExpectNoError(err)
	}
	return &r
}
