// This file is part of MinIO DirectPV
// Copyright (c) 2022 MinIO, Inc.
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

	directpvtypes "github.com/minio/directpv/pkg/apis/directpv.min.io/types"
	"github.com/minio/directpv/pkg/client"
	"github.com/minio/directpv/pkg/device"
	"github.com/minio/directpv/pkg/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

func createOrUpdateNode(ctx context.Context, nodeID directpvtypes.NodeID, devices []types.Device) error {
	node, err := client.NodeClient().Get(ctx, string(nodeID), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		_, err = client.NodeClient().Create(ctx, types.NewNode(nodeID, devices), metav1.CreateOptions{})
		return err
	}

	node.Status.Devices = devices
	node.Spec.Refresh = false
	_, err = client.NodeClient().Update(ctx, node, metav1.UpdateOptions{TypeMeta: types.NewNodeTypeMeta()})
	return err
}

// Sync updates node CRD with current devices.
func Sync(ctx context.Context, nodeID directpvtypes.NodeID) error {
	probedDevices, err := device.Probe()
	if err != nil {
		return err
	}
	var devices []types.Device
	for i := range probedDevices {
		devices = append(devices, probedDevices[i].ToNodeDevice(nodeID))
	}

	return retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error { return createOrUpdateNode(ctx, nodeID, devices) },
	)
}
