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
	"strconv"
	"strings"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/utils"
	"github.com/minio/directpv/pkg/client"
	"k8s.io/client-go/util/retry"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"path/filepath"

	"k8s.io/klog/v2"
)

var capacity bool
var checkFinalizer bool
var diff int64

var checkDrivesCmd = &cobra.Command{
	Use:   "check",
	Short: utils.BinaryNameTransform("check drives in the {{ . }} cluster"),
	Long:  "",
	Example: utils.BinaryNameTransform(`
# List all drives
$ kubectl {{ . }} drives ls

# List all drives (including 'unavailable' drives)
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
		if len(driveGlobs) > 0 || len(nodeGlobs) > 0 || len(statusGlobs) > 0 {
			klog.Warning("Glob matches will be deprecated soon. Please use ellipses instead")
		}
		return checkDrives(c.Context(), args)
	},
	Aliases: []string{
		"check",
	},
}

func init() {
	checkDrivesCmd.PersistentFlags().StringSliceVarP(&drives, "drives", "d", drives, "filter by drive path(s) (also accepts ellipses range notations)")
	checkDrivesCmd.PersistentFlags().StringSliceVarP(&nodes, "nodes", "n", nodes, "filter by node name(s) (also accepts ellipses range notations)")
	checkDrivesCmd.PersistentFlags().StringSliceVarP(&status, "status", "s", status, fmt.Sprintf("match based on drive status [%s]", strings.Join(directcsi.SupportedStatusSelectorValues(), ", ")))
	checkDrivesCmd.PersistentFlags().BoolVarP(&all, "all", "a", all, "list all drives (including unavailable)")
	checkDrivesCmd.PersistentFlags().StringSliceVarP(&accessTiers, "access-tier", "", accessTiers, "match based on access-tier")
	checkDrivesCmd.PersistentFlags().BoolVarP(&capacity, "capacity", "c", capacity, "check capacity of the drives")
	checkDrivesCmd.PersistentFlags().Int64VarP(&diff, "diff", "", diff, "diff of cap to be fixed")
	checkDrivesCmd.PersistentFlags().BoolVarP(&repair, "repair", "", repair, "repair drives")
	checkDrivesCmd.PersistentFlags().BoolVarP(&checkFinalizer, "check-finalizer", "", checkFinalizer, "chjeck finalizers in the drives")
}

func checkDrives(ctx context.Context, args []string) error {
	filteredDrives, err := getFilteredDriveList(
		ctx,
		func(drive directcsi.DirectCSIDrive) bool {
			if len(driveStatusList) > 0 {
				return drive.MatchDriveStatus(driveStatusList)
			}
			return all || len(statusGlobs) > 0 || drive.Status.DriveStatus != directcsi.DriveStatusUnavailable
		},
	)
	if err != nil {
		return err
	}

	uniqFSUUIDs := make(map[string]string)
	uniqPartitionUUIDs := make(map[string]string)
	uniqWWIDPartNo := make(map[string]string)
	for _, d := range filteredDrives {
		if d.Status.FilesystemUUID != "" {
			if v, ok := uniqFSUUIDs[d.Status.FilesystemUUID]; ok {
				klog.Infof("\n[INVALID] Duplicate FSUUID (%s) found for drives %s and %s", d.Status.FilesystemUUID, v, d.Name)
			} else {
				uniqFSUUIDs[d.Status.FilesystemUUID] = d.Name
			}
		}
		if d.Status.PartitionUUID != "" {
			if v, ok := uniqPartitionUUIDs[d.Status.PartitionUUID]; ok {
				klog.Infof("\n[INVALID] Duplicate PartitionUUID (%s) found for drives %s and %s", d.Status.FilesystemUUID, v, d.Name)
			} else {
				uniqPartitionUUIDs[d.Status.PartitionUUID] = d.Name
			}
		}
		if d.Status.WWID != "" {
			key := strings.Join(
				[]string{
					d.Status.WWID,
					strconv.Itoa(d.Status.PartitionNum),
				},
				"-",
			)
			if v, ok := uniqWWIDPartNo[key]; ok {
				klog.Infof("\n[INVALID] Duplicate WWID (%s) found for drives %s and %s", key, v, d.Name)
			} else {
				uniqWWIDPartNo[key] = d.Name
			}
		}
		switch d.Status.DriveStatus {
		case directcsi.DriveStatusReady, directcsi.DriveStatusInUse:
			if d.Status.Filesystem == "" || d.Status.Filesystem != "xfs" {
				klog.Infof("\n[INVALID] invalid fstype for managed drive: %s found %s", d.Name, d.Status.Filesystem)
			}
			if d.Status.FilesystemUUID == "" {
				klog.Infof("\n[INVALID] empty fsuuid for managed drive: %s", d.Name)
			}
			if filepath.Base(d.Status.Mountpoint) != d.Status.FilesystemUUID {
				klog.Infof("\n[INVALID] invalid mountpoint/fsuuid for managed drive: %s", d.Name)
			}
			if checkFinalizer && d.Status.DriveStatus == directcsi.DriveStatusInUse {
				if !repair {
					unexpectedFinalizerPresent, err := hasUnexpectedFinalizers(ctx, d.Name)
					if err != nil {
						return err
					}
					if unexpectedFinalizerPresent {
						klog.Infof("\n[INVALID] unexpected finalizers present for the drive: %s", d.Name)
					}
				} else {
					err = removeUnexpectedFinalizers(ctx, d.Name)
					if err != nil {
						klog.Infof("\ncouldn't remove unexpected finalizers from drive: %s: %v", d.Name, err)
						return err
					}
				}
			}
			if capacity && d.Status.DriveStatus == directcsi.DriveStatusInUse {
				var diffBytes int64
				dr, err := client.GetLatestDirectCSIDriveInterface().Get(
					ctx, d.Name, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
				)
				allocatedCapacity, err := getAllocatedCapacityFromVolumes(ctx, dr)
				if err != nil {
					return err
				}
				metadataSize := int64(0.0005*float32(dr.Status.TotalCapacity))
				allocatedCapacity = allocatedCapacity+metadataSize
				if dr.Status.AllocatedCapacity == allocatedCapacity {
					continue
				}
				if !repair {
					if dr.Status.AllocatedCapacity < allocatedCapacity {
						diffBytes = allocatedCapacity - dr.Status.AllocatedCapacity
						if diff != int64(0) {
						 if diffBytes > diff {
							klog.Infof("\n [LESS THAN EXPECTED CAP] drive: %s allocatedCap: %v drive.AllocatedCap: %v diffInBytes: %v mdsize: %v", dr.Name, allocatedCapacity, dr.Status.AllocatedCapacity, diffBytes, metadataSize)
						 }
						} else {
							klog.Infof("\n [LESS THAN EXPECTED CAP] drive: %s allocatedCap: %v drive.AllocatedCap: %v diffInBytes: %v mdsize: %v", dr.Name, allocatedCapacity, dr.Status.AllocatedCapacity, diffBytes, metadataSize)
						}
					}
					if allocatedCapacity < dr.Status.AllocatedCapacity {
						diffBytes = dr.Status.AllocatedCapacity - allocatedCapacity
						if diff != int64(0) { 
						  if diffBytes > diff {
							klog.Infof("\n [MORE THAN EXPECTED CAP] drive: %s allocatedCap: %v drive.AllocatedCap: %v diffInBytes: %v mdsize: %v", dr.Name, allocatedCapacity, dr.Status.AllocatedCapacity, diffBytes, metadataSize)
						  }
						} else {
							klog.Infof("\n [MORE THAN EXPECTED CAP] drive: %s allocatedCap: %v drive.AllocatedCap: %v diffInBytes: %v mdsize: %v", dr.Name, allocatedCapacity, dr.Status.AllocatedCapacity, diffBytes, metadataSize)
						}
					}
				}
				if repair {
					if err := retry.RetryOnConflict(retry.DefaultRetry, updateDriveAllocatedCapacity(ctx, dr.Name, allocatedCapacity)); err != nil {
						klog.Errorf("Error while setting allocatedCap for drive: %s: %v", dr.Name, err)
					}
				}
			}

		}
	}

	return nil
}

func updateDriveAllocatedCapacity(ctx context.Context, driveName string, allocatedCapacity int64) func() error {
	return func() error {
		driveClient := client.GetLatestDirectCSIDriveInterface()
		drive, err := driveClient.Get(
			ctx, driveName, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
		)
		if err != nil {
			return err
		}
		drive.Status.AllocatedCapacity = allocatedCapacity
		_, err = driveClient.Update(
			ctx, drive, metav1.UpdateOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
		)
		return err
	}
}

func getAllocatedCapacityFromVolumes(ctx context.Context, drive *directcsi.DirectCSIDrive) (int64, error) {
	var totalVolumeCapacityOnTheDrive int64
	for _, finalizer := range drive.GetFinalizers() {
		if !strings.HasPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix) {
			continue
		}
		volumeNameFromFinalizer := strings.TrimPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix)
		volume, err := client.GetLatestDirectCSIVolumeInterface().Get(
			ctx, volumeNameFromFinalizer, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return totalVolumeCapacityOnTheDrive, err
			}
			continue
		}
		totalVolumeCapacityOnTheDrive = totalVolumeCapacityOnTheDrive + volume.Status.TotalCapacity
	}

	return totalVolumeCapacityOnTheDrive, nil
}

func hasUnexpectedFinalizers(ctx context.Context, driveName string) (bool, error) {
	drive, err := client.GetLatestDirectCSIDriveInterface().Get(
		ctx, driveName, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
	)
	if err != nil {
		return true, err
	}
	for _, finalizer := range drive.GetFinalizers() {
		if !strings.HasPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix) {
			continue
		}
		volumeNameFromFinalizer := strings.TrimPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix)
		volume, err := client.GetLatestDirectCSIVolumeInterface().Get(
			ctx, volumeNameFromFinalizer, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return true, err
		}
		if volume.Status.Drive != drive.Name {
			return true, nil
		}
	}
	return false, nil
}

func removeUnexpectedFinalizers(ctx context.Context, driveName string) error {
	drive, err := client.GetLatestDirectCSIDriveInterface().Get(
		ctx, driveName, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
	)
	if err != nil {
		return err
	}
	validFinalizers := []string{}
	for _, finalizer := range drive.GetFinalizers() {
		if !strings.HasPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix) {
			validFinalizers = append(validFinalizers, finalizer)
			continue
		}
		volumeNameFromFinalizer := strings.TrimPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix)
		volume, err := client.GetLatestDirectCSIVolumeInterface().Get(
			ctx, volumeNameFromFinalizer, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
		)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if volume.Status.Drive == drive.Name {
			validFinalizers = append(validFinalizers, finalizer)
		}
	}
	klog.Infof("\n setting finalizers: %v for drive: %s", validFinalizers, drive.Name)
	if err := retry.RetryOnConflict(retry.DefaultRetry, setValidFinazers(ctx, drive.Name, validFinalizers)); err != nil {
		klog.Errorf("Error while setting valid finalizers for drive: %s with finalizers: %v: %v", drive.Name, validFinalizers, err)
		return err
	}
	return nil
}

func setValidFinazers(ctx context.Context, driveName string, finalizers []string) func() error {
	return func() error {
		driveClient := client.GetLatestDirectCSIDriveInterface()
		drive, err := driveClient.Get(
			ctx, driveName, metav1.GetOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
		)
		drive.SetFinalizers(finalizers)
		if len(finalizers) == 1 {
			if finalizers[0] == directcsi.DirectCSIDriveFinalizerDataProtection {
				drive.Status.DriveStatus = directcsi.DriveStatusReady
			}
		}
		_, err = driveClient.Update(
			ctx, drive, metav1.UpdateOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
		)
		return err
	}
}