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

package client

import (
	"github.com/minio/directpv/pkg/clientset/fake"
	"github.com/minio/directpv/pkg/k8s"
	"github.com/minio/directpv/pkg/types"
)

var fakeMode bool

// SetFakeMode sets fakeMode which uses only fake clients
func SetFakeMode() {
	fakeMode = true
	k8s.SetFakeMode()
}

// NewFakeClient initializes fake clients.
func NewFakeClient() (*Client, error) {
	k8sClient, err := k8s.NewFakeClient()
	if err != nil {
		return nil, err
	}

	clientsetInterface := types.NewExtFakeClientset(fake.NewSimpleClientset())
	driveClient := clientsetInterface.DirectpvLatest().DirectPVDrives()
	volumeClient := clientsetInterface.DirectpvLatest().DirectPVVolumes()
	nodeClient := clientsetInterface.DirectpvLatest().DirectPVNodes()
	initRequestClient := clientsetInterface.DirectpvLatest().DirectPVInitRequests()
	restClient := clientsetInterface.DirectpvLatest().RESTClient()

	initEvent(k8sClient.KubeClient)
	return &Client{
		KubernetesClient:   k8sClient,
		ClientsetInterface: clientsetInterface,
		RESTClient:         restClient,
		DriveClient:        driveClient,
		VolumeClient:       volumeClient,
		NodeClient:         nodeClient,
		InitRequestClient:  initRequestClient,
	}, nil
}

// SetDriveInterface sets latest drive interface.
// Note: To be used for writing test cases only
func (c *Client) SetDriveInterface(i types.LatestDriveInterface) {
	if !fakeMode {
		return
	}
	c.DriveClient = i
}

// SetVolumeInterface sets the latest volume interface.
// Note: To be used for writing test cases only
func (c *Client) SetVolumeInterface(i types.LatestVolumeInterface) {
	if !fakeMode {
		return
	}
	c.VolumeClient = i
}

// SetNodeInterface sets latest node interface.
// Note: To be used for writing test cases only
func (c *Client) SetNodeInterface(i types.LatestNodeInterface) {
	if !fakeMode {
		return
	}
	c.NodeClient = i
}

// SetInitRequestInterface sets latest initrequest interface.
// Note: To be used for writing test cases only
func (c *Client) SetInitRequestInterface(i types.LatestInitRequestInterface) {
	if !fakeMode {
		return
	}
	c.InitRequestClient = i
}
