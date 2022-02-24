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

package node

import (
	"context"

	"github.com/google/uuid"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/client"
	"github.com/minio/directpv/pkg/sys"
	"github.com/minio/directpv/pkg/uevent"
	"github.com/minio/directpv/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

func RunDynamicDriveHandler(ctx context.Context,
	identity, nodeID, rack, zone, region string,
	loopbackOnly bool) error {

	handler := &driveEventHandler{
		nodeID: nodeID,
		topology: map[string]string{
			string(utils.TopologyDriverIdentity): identity,
			string(utils.TopologyDriverRack):     rack,
			string(utils.TopologyDriverZone):     zone,
			string(utils.TopologyDriverRegion):   region,
			string(utils.TopologyDriverNode):     nodeID,
		},
	}

	return uevent.Run(ctx, nodeID, handler)
}

type driveEventHandler struct {
	nodeID   string
	topology map[string]string
}

func (d *driveEventHandler) Add(ctx context.Context, device *sys.Device) error {
	drive := client.NewDirectCSIDrive(
		uuid.New().String(),
		client.NewDirectCSIDriveStatus(device, d.nodeID, d.topology),
	)
	err := client.CreateDrive(ctx, drive)
	if err != nil {
		klog.ErrorS(err, "unable to create drive", "Status.Path", drive.Status.Path)
	}
	return err
}

func (d *driveEventHandler) Change(ctx context.Context, device *sys.Device, drive *directcsi.DirectCSIDrive) error {
	var errMessage string
	updatedDrive, err := d.syncDrive(device, drive)
	if err != nil {
		errMessage = err.Error()
	} else {
		if drive.Status.Path != updatedDrive.Status.Path {
			if err := syncVolumeLabels(ctx, updatedDrive); err != nil {
				return err
			}
		}
	}
	utils.UpdateCondition(updatedDrive.Status.Conditions,
		string(directcsi.DirectCSIDriveConditionInitialized),
		utils.BoolToCondition(errMessage == ""),
		string(directcsi.DirectCSIDriveReasonInitialized),
		errMessage)

	_, err = client.GetLatestDirectCSIDriveInterface().Update(ctx, updatedDrive, metav1.UpdateOptions{})
	return err
}

func (d *driveEventHandler) Remove(ctx context.Context, device *sys.Device, drive *directcsi.DirectCSIDrive) error {
	return nil
}
