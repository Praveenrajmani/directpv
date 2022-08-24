// This file is part of MinIO DirectPV
// Copyright (c) 2021, 2022 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/client"
	"github.com/minio/directpv/pkg/matcher"
	"github.com/minio/directpv/pkg/utils"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	"github.com/spf13/cobra"
)

var checkVolumesCmd = &cobra.Command{
	Use:   "check",
	Short: utils.BinaryNameTransform("check volumes in the {{ . }} cluster"),
	Long:  "",
	Example: utils.BinaryNameTransform(`

# List all staged and published volumes
$ kubectl {{ . }} volumes check --status=staged,published

# List all volumes from a particular node
$ kubectl {{ . }} volumes check --nodes=direct-1

# Combine multiple filters using csv
$ kubectl {{ . }} vol check --nodes=direct-1,direct-2 --status=staged --drives=/dev/nvme0n1

# List all published volumes by pod name
$ kubectl {{ . }} volumes check --status=published --pod-name=minio-{1...3}

# List all published volumes by pod namespace
$ kubectl {{ . }} volumes check --status=published --pod-namespace=tenant-{1...3}

# List all volumes provisioned based on drive and volume ellipses
$ kubectl {{ . }} volumes check --drives '/dev/xvd{a...d} --nodes 'node-{1...4}''

`),
	RunE: func(c *cobra.Command, args []string) error {
		if err := validateVolumeSelectors(); err != nil {
			return err
		}
		if len(driveGlobs) > 0 || len(nodeGlobs) > 0 || len(podNameGlobs) > 0 || len(podNsGlobs) > 0 {
			klog.Warning("Glob matches will be deprecated soon. Please use ellipses instead")
		}
		return checkVolumes(c.Context(), args)
	},
	Aliases: []string{
		"ls",
	},
}

var repair bool
var ignoreMountErrs bool

func init() {
	checkVolumesCmd.PersistentFlags().StringSliceVarP(&drives, "drives", "d", drives, "filter by drive path(s) (also accepts ellipses range notations)")
	checkVolumesCmd.PersistentFlags().StringSliceVarP(&nodes, "nodes", "n", nodes, "filter by node name(s) (also accepts ellipses range notations)")
	checkVolumesCmd.PersistentFlags().StringSliceVarP(&volumeStatus, "status", "s", volumeStatus, "match based on volume status. The possible values are [staged,published]")
	checkVolumesCmd.PersistentFlags().StringSliceVarP(&podNames, "pod-name", "", podNames, "filter by pod name(s) (also accepts ellipses range notations)")
	checkVolumesCmd.PersistentFlags().StringSliceVarP(&podNss, "pod-namespace", "", podNss, "filter by pod namespace(s) (also accepts ellipses range notations)")
	checkVolumesCmd.PersistentFlags().BoolVarP(&all, "all", "a", all, "list all volumes (including non-provisioned)")
	checkVolumesCmd.PersistentFlags().BoolVarP(&repair, "repair", "r", repair, "repair volumes")
	checkVolumesCmd.PersistentFlags().BoolVarP(&ignoreMountErrs, "ignore-mount-errs", "", ignoreMountErrs, "ignore mount errors")
}

func checkVolumes(ctx context.Context, args []string) error {

	volumeList, err := getFilteredVolumeList(
		ctx,
		func(volume directcsi.DirectCSIVolume) bool {
			return all || utils.IsConditionStatus(volume.Status.Conditions, string(directcsi.DirectCSIVolumeConditionReady), metav1.ConditionTrue)
		},
	)
	if err != nil {
		return err
	}

	for _, volume := range volumeList {
		if volume.Status.HostPath == "" {
			klog.Infof("\n empty hostpath: %s", volume.Name)
			pv, err := client.GetKubeClient().CoreV1().PersistentVolumes().Get(ctx, volume.Name, metav1.GetOptions{})
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return err
				}
				if repair {
					klog.Infof("force removing stale volume: %s", pv.Name)
					if err := retry.RetryOnConflict(retry.DefaultRetry, forceRemoveVolume(ctx, volume.Name)); err != nil {
						klog.Errorf("Error while force removing volume: %s: %v", volume.Name, err)
					}
				}
			}
			continue
		}
		drive, err := client.GetLatestDirectCSIDriveInterface().Get(
			ctx, volume.Status.Drive, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
		)
		if err != nil {
			klog.Errorf("Error while fetching drive: %s for volume: %s", volume.Status.Drive, volume.Name)
			continue
		}
		hostPath := filepath.Join(drive.Status.Mountpoint, volume.Name)
		if !repair && !strings.HasPrefix(volume.Status.HostPath, drive.Status.Mountpoint) {
			klog.Errorf("[INVALID] Invalid hostpath prefix. expected: %s but found %s", drive.Status.Mountpoint, volume.Status.HostPath)
		}
		if hostPath != volume.Status.HostPath {
			if !repair {
				klog.Errorf("\n [NEEDS FIX] volume: %s, Expected hostpath: %s but got %s, containerPath: %s, drive: %s, node: %s, path: %s, maj-min: %v:%v",
					volume.Name,
					hostPath,
					volume.Status.HostPath,
					volume.Status.ContainerPath,
					drive.Name,
					drive.Status.NodeName,
					drive.Status.Path,
					drive.Status.MajorNumber,
					drive.Status.MinorNumber,
				)
			}
			if repair {
				if err := retry.RetryOnConflict(retry.DefaultRetry, setCorrectDriveByFSUUID(ctx, volume.Name)); err != nil {
					klog.Errorf("Error while setting drive for volume: %s: %v", volume.Name, err)
				}
			}
			continue
		}
		foundFinalizer := false
		finalizer := directcsi.DirectCSIDriveFinalizerPrefix + volume.Name
		for _, f := range drive.GetFinalizers() {
			if f == finalizer {
				foundFinalizer = true
			}
		}
		if !foundFinalizer {
			klog.Errorf("[INVALID] finalizer %s for volume not found in %s", finalizer, drive.Name)
		}
	}

	return nil
}

func setCorrectDriveByFSUUID(ctx context.Context, volumeName string) func() error {
	return func() error {
		volumeClient := client.GetLatestDirectCSIVolumeInterface()
		volume, err := volumeClient.Get(
			ctx, volumeName, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			return err
		}

		fsuuid := getFSUUIDFromHostPath(volume.Status.HostPath, volume.Name)
		filteredDrives, err := getFilteredDriveList(
			ctx,
			func(drive directcsi.DirectCSIDrive) bool {
				if fsuuid != "" {
					return drive.Status.FilesystemUUID == fsuuid
				}
				return false
			},
		)
		if err != nil {
			return err
		}

		if len(filteredDrives) == 0 {
			return fmt.Errorf("couldn't find drive matching fsuuid %s", fsuuid)
		}

		if len(filteredDrives) != 1 {
			return fmt.Errorf("found more than one matching drives for fsuuid: %s", fsuuid)
		}

		expectedMountpoint := "/var/lib/direct-csi/mnt/" + fsuuid
		if !ignoreMountErrs && filteredDrives[0].Status.Mountpoint != expectedMountpoint {
			return fmt.Errorf("mountpoint mismatch - expected: %s but got: %s", expectedMountpoint, filteredDrives[0].Status.Mountpoint)
		}

		if err := unsetExistingFinalizer(ctx, volumeName, volume.Status.Drive); err != nil {
			klog.Errorf("error while unsetting finalizers from the drive: %s for volume: %s: %v", volume.Status.Drive, volume.Name, err)
			return err
		}

		if err := fixDriveForVolume(ctx, volume.Name, filteredDrives[0].Name, volume.Status.TotalCapacity); err != nil {
			klog.Errorf("error while fixing the drive: %s for volume: %s: %v", filteredDrives[0].Name, volume.Name, err)
			return err
		}

		volume.Status.Drive = filteredDrives[0].Name
		_, err = volumeClient.Update(
			ctx, volume, metav1.UpdateOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		return err
	}
}

func getFSUUIDFromHostPath(hostPath, volumeName string) string {
	// /var/lib/direct-csi/mnt/220b5a17-4716-4341-aeab-8e5ed45982e5/pvc-dbd3912a-35e2-4dfb-a260-d6ad02e2493
	trimmedStr := strings.TrimPrefix(hostPath, "/var/lib/direct-csi/mnt/")
	fsuuid := strings.TrimSuffix(trimmedStr, "/"+volumeName)
	return fsuuid
}

func fixDriveForVolume(ctx context.Context, volumeName string, driveName string, size int64) error {
	driveClient := client.GetLatestDirectCSIDriveInterface()
	drive, err := driveClient.Get(
		ctx, driveName, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
	)
	if err != nil {
		return err
	}
	drive.Status.DriveStatus = directcsi.DriveStatusInUse
	finalizer := directcsi.DirectCSIDriveFinalizerPrefix + volumeName
	if !matcher.StringIn(drive.Finalizers, finalizer) {
		drive.Status.FreeCapacity -= size
		drive.Status.AllocatedCapacity += size
		drive.SetFinalizers(append(drive.GetFinalizers(), finalizer))
	}
	_, err = driveClient.Update(
		ctx, drive, metav1.UpdateOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
	)
	return err

}

func unsetExistingFinalizer(ctx context.Context, volumeName string, driveName string) error {
	driveClient := client.GetLatestDirectCSIDriveInterface()
	drive, err := driveClient.Get(
		ctx, driveName, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
	)
	if err != nil {
		return err
	}
	finalizer := directcsi.DirectCSIDriveFinalizerPrefix + volumeName
	expectedFinalizers := []string{}
	var found bool
	if matcher.StringIn(drive.Finalizers, finalizer) {
		found = true
		for _, f := range drive.Finalizers {
			if f != finalizer {
				expectedFinalizers = append(expectedFinalizers, f)
			} else {
				klog.Infof("\n removing finalizer: %s from drive: %s", f, drive.Name)
			}
		}
		drive.SetFinalizers(expectedFinalizers)
	}
	if found {
		_, err = driveClient.Update(
			ctx, drive, metav1.UpdateOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
		)
		return err
	}
	return nil
}

func setVolumeHostPath(ctx context.Context, volumeName, hostPath string) func() error {
	return func() error {
		volumeClient := client.GetLatestDirectCSIVolumeInterface()
		volume, err := volumeClient.Get(
			ctx, volumeName, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			return err
		}
		volume.Status.HostPath = hostPath
		_, err = volumeClient.Update(
			ctx, volume, metav1.UpdateOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		return err
	}
}

func forceRemoveVolume(ctx context.Context, volumeName string) func() error {
	return func() error {
		volumeClient := client.GetLatestDirectCSIVolumeInterface()
		volume, err := volumeClient.Get(
			ctx, volumeName, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			return err
		}
		volume.SetFinalizers([]string{})
		if _, err := volumeClient.Update(
			ctx, volume, metav1.UpdateOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		); err != nil {
			return err
		}
		if err := volumeClient.Delete(ctx, volumeName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
}
