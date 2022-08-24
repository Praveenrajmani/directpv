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
	"github.com/minio/directpv/pkg/utils"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/client-go/util/retry"
)

var reclaimDrivesCmd = &cobra.Command{
	Use:   "reclaim",
	Short: utils.BinaryNameTransform("reclaim drives in the {{ . }} cluster"),
	Long:  "",
	Example: utils.BinaryNameTransform(`
# reclaim all drives
$ kubectl {{ . }} drives ls

# Reclaim all drives (including 'unavailable' drives)
$ kubectl {{ . }} drives ls --all

# Filter all ready drives 
$ kubectl {{ . }} drives ls --status=ready

# Filter all drives from a particular node
$ kubectl {{ . }} drives ls --nodes=direct-1

# Combine multiple filters using multi-arg
$ kubectl {{ . }} drives ls --nodes=direct-1 --nodes=othernode-2 --status=available

# Combine multiple filters using csv
$ kubectl {{ . }} drives ls --nodes=direct-1,othernode-2 --status=ready

# Filter all drives based on access-tier
$ kubectl {{ . }} drives drives ls --access-tier="hot"

# Filter all drives with access-tier being set
$ kubectl {{ . }} drives drives ls --access-tier="*"

# Filter drives by ellipses notation for drive paths and nodes
$ kubectl {{ . }} drives ls --drives='/dev/xvd{a...d}' --nodes='node-{1...4}'
`),
	RunE: func(c *cobra.Command, args []string) error {
		if err := validateDriveSelectors(); err != nil {
			return err
		}
		if !all {
			if len(drives) == 0 && len(nodes) == 0 && len(accessTiers) == 0 && len(args) == 0 {
				return fmt.Errorf("atleast one of '%s', '%s' or '%s' must be specified",
					utils.Bold("--all"),
					utils.Bold("--drives"),
					utils.Bold("--nodes"))
			}
		}
		if len(driveGlobs) > 0 || len(nodeGlobs) > 0 || len(statusGlobs) > 0 {
			klog.Warning("Glob matches will be deprecated soon. Please use ellipses instead")
		}
		return reclaimDrives(c.Context(), args)
	},
	Aliases: []string{
		"rclm",
	},
}

func init() {
	reclaimDrivesCmd.PersistentFlags().StringSliceVarP(&drives, "drives", "d", drives, "filter by drive path(s) (also accepts ellipses range notations)")
	reclaimDrivesCmd.PersistentFlags().StringSliceVarP(&nodes, "nodes", "n", nodes, "filter by node name(s) (also accepts ellipses range notations)")
	reclaimDrivesCmd.PersistentFlags().BoolVarP(&all, "all", "a", all, "list all drives (including unavailable)")
	reclaimDrivesCmd.PersistentFlags().StringSliceVarP(&accessTiers, "access-tier", "", accessTiers, "match based on access-tier")
	reclaimDrivesCmd.PersistentFlags().BoolVarP(&dryRun, "dry-run", "", dryRun, "run reclaim drive with dryRun")
}

func getVolumeSizeFromDrive(ctx context.Context, drive *directcsi.DirectCSIDrive, volumeName string) (int64, error) {
	var totalVolumeCapacityOnTheDrive, volumeSize int64
	for _, finalizer := range drive.GetFinalizers() {
		if !strings.HasPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix) {
			continue
		}
		volumeNameFromFinalizer := strings.TrimPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix)
		if volumeNameFromFinalizer == volumeName {
			continue
		}
		volume, err := client.GetLatestDirectCSIVolumeInterface().Get(
			ctx, volumeNameFromFinalizer, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return volumeSize, err
			}
			continue
		}
		totalVolumeCapacityOnTheDrive = totalVolumeCapacityOnTheDrive + volume.Status.TotalCapacity
	}

	if drive.Status.AllocatedCapacity > totalVolumeCapacityOnTheDrive {
		volumeSize = drive.Status.AllocatedCapacity - totalVolumeCapacityOnTheDrive
		drive.Status.AllocatedCapacity = totalVolumeCapacityOnTheDrive
	}

	return volumeSize, nil
}

func isStaleVolume(ctx context.Context, volumeName string) (bool, int64, error) {
	var volumeCapacity int64
	_, err := client.GetLatestDirectCSIVolumeInterface().Get(
		ctx, volumeName, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
	)
	if err == nil {
		return false, volumeCapacity, nil
	}
	klog.Infof("volume: %s, err: %s", volumeName, err)
	if !apierrors.IsNotFound(err) {
		return false, volumeCapacity, err
	}
	
	pv, err := client.GetKubeClient().CoreV1().PersistentVolumes().Get(ctx, volumeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, volumeCapacity, nil
		}
		klog.Infof("couldn't fetch the pv %s due to %s", volumeName, err.Error())
		return false, volumeCapacity, err
	}
	switch pv.Status.Phase {
	case corev1.VolumeReleased, corev1.VolumeFailed:
		// return true, volumeCapacity, nil
	default:
		klog.Infof("couldn't reclaim the volume %s as the pv is in %s phase", pv.Name, string(pv.Status.Phase))
		return false, volumeCapacity, nil
	}
	volumeCapacity = pv.Spec.Capacity.Storage().Value()
	return true, volumeCapacity, nil
}

func reclaimDrives(ctx context.Context, args []string) error {
	filteredDrives, err := getFilteredDriveList(
		ctx,
		func(drive directcsi.DirectCSIDrive) bool {
			return drive.Status.DriveStatus == directcsi.DriveStatusInUse
		},
	)
	if err != nil {
		return err
	}

	var staleDirectCSIVolumes []directcsi.DirectCSIVolume

	for _, drive := range filteredDrives {
		for _, finalizer := range drive.GetFinalizers() {
			if !strings.HasPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix) {
				continue
			}

			volumeName := strings.TrimPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix)
			isStale, volumeSize, err := isStaleVolume(ctx, volumeName)
			if err != nil {
				return err
			}

			if isStale {
				if volumeSize == int64(0) {
					volumeSize, err = getVolumeSizeFromDrive(ctx, &drive, volumeName)
					if err != nil {
						return err
					}
				}
				newVolume := directcsi.DirectCSIVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: volumeName,
						Finalizers: []string{
							directcsi.DirectCSIVolumeFinalizerPurgeProtection,
						},
						Labels: map[string]string{
							string(utils.NodeLabelKey): drive.Status.NodeName,
						},
					},
					Status: directcsi.DirectCSIVolumeStatus{
						Drive:         drive.Name,
						HostPath:      filepath.Join(drive.Status.Mountpoint, volumeName),
						TotalCapacity: volumeSize,
					},
				}
				staleDirectCSIVolumes = append(staleDirectCSIVolumes, newVolume)
			}
		}
	}

	for _, volume := range staleDirectCSIVolumes {
		if _, err := client.GetLatestDirectCSIVolumeInterface().Create(ctx, &volume, metav1.CreateOptions{}); err != nil {
			return err
		}
		if err = client.GetLatestDirectCSIVolumeInterface().Delete(ctx, volume.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
		if dryRun {
			klog.Infof("\n stale volume: %s with hostpath: %s, size: %v, drive: %s", volume.Name, volume.Status.HostPath, volume.Status.TotalCapacity, volume.Status.Drive)
			if err := retry.RetryOnConflict(retry.DefaultRetry, dummyDelete(ctx, &volume)); err != nil {
				klog.Errorf("Error while doing dummy delete for volume: %s: %v", volume.Name, err)
			}
			continue
		}
	}

	return nil
}


func dummyDelete(ctx context.Context, v *directcsi.DirectCSIVolume) func() error {
	return func() error {
		volume, err := client.GetLatestDirectCSIVolumeInterface().Get(
			ctx, v.Name, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			return err
		}
		finalizers, _ := excludeFinalizer(
			volume.GetFinalizers(), string(directcsi.DirectCSIVolumeFinalizerPurgeProtection),
		)
		if len(finalizers) > 0 {
			return fmt.Errorf("waiting for the volume to be released before cleaning up")
		}
	
		// Release volume from associated drive.
		if err := dummyReleaseVolume(ctx, volume.Status.Drive, volume.Name, volume.Status.TotalCapacity); err != nil {
			return err
		}
	
		volume.SetFinalizers(finalizers)
		_, err = client.GetLatestDirectCSIVolumeInterface().Update(
			ctx, volume, metav1.UpdateOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		return err
	}
}

func dummyReleaseVolume(ctx context.Context, driveName, volumeName string, capacity int64) error {
	driveInterface := client.GetLatestDirectCSIDriveInterface()
	drive, err := driveInterface.Get(
		ctx, driveName, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
	)
	if err != nil {
		return err
	}

	finalizers, found := excludeFinalizer(
		drive.GetFinalizers(), directcsi.DirectCSIDriveFinalizerPrefix+volumeName,
	)

	if found {
		if len(finalizers) == 1 {
			if finalizers[0] == directcsi.DirectCSIDriveFinalizerDataProtection {
				drive.Status.DriveStatus = directcsi.DriveStatusReady
			}
		}

		drive.SetFinalizers(finalizers)
		drive.Status.FreeCapacity += capacity
		drive.Status.AllocatedCapacity = drive.Status.TotalCapacity - drive.Status.FreeCapacity

		_, err = driveInterface.Update(
			ctx, drive, metav1.UpdateOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
		)
	}

	return err
}

func excludeFinalizer(finalizers []string, finalizer string) (result []string, found bool) {
	for _, f := range finalizers {
		if f != finalizer {
			result = append(result, f)
		} else {
			found = true
		}
	}
	return
}