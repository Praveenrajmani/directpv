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
	"fmt"
	"log"

	"github.com/minio/directpv/pkg/clientset"
	"github.com/minio/directpv/pkg/k8s"
	"github.com/minio/directpv/pkg/types"
	"k8s.io/client-go/rest"
)

// Client represents the DirectPV client set.
type Client struct {
	KubernetesClient   *k8s.Client
	ClientsetInterface types.ExtClientsetInterface
	RESTClient         rest.Interface
	DriveClient        types.LatestDriveInterface
	VolumeClient       types.LatestVolumeInterface
	NodeClient         types.LatestNodeInterface
	InitRequestClient  types.LatestInitRequestInterface
}

// K8s will return the kubernetes client
func (client Client) K8s() *k8s.Client {
	return client.KubernetesClient
}

// NewClient initializes and returns the DirectPV client.
func NewClient() (*Client, error) {
	if fakeMode {
		return NewFakeClient()
	}
	k8sClient, err := k8s.NewClient()
	if err != nil {
		log.Fatalf("unable to initialize k8s clients; %v", err)
	}
	return newClient(k8sClient)
}

// NewClientWithConfig initializes and returns the DirectPV client with the provided kube config.
func NewClientWithConfig(c *rest.Config) (*Client, error) {
	if fakeMode {
		return NewFakeClient()
	}
	k8sClient, err := k8s.NewClientWithConfig(c)
	if err != nil {
		return nil, err
	}
	return newClient(k8sClient)
}

func newClient(k8s *k8s.Client) (*Client, error) {
	cs, err := clientset.NewForConfig(k8s.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new clientset interface; %v", err)
	}
	clientsetInterface := types.NewExtClientset(cs)
	restClient := clientsetInterface.DirectpvLatest().RESTClient()
	driveClient, err := latestDriveClientForConfig(k8s.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new drive interface; %v", err)
	}
	volumeClient, err := latestVolumeClientForConfig(k8s.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new volume interface; %v", err)
	}
	nodeClient, err := latestNodeClientForConfig(k8s.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new node interface; %v", err)
	}
	initRequestClient, err := latestInitRequestClientForConfig(k8s.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new initrequest interface; %v", err)
	}
	initEvent(k8s.KubeClient)
	return &Client{
		KubernetesClient:   k8s,
		ClientsetInterface: clientsetInterface,
		RESTClient:         restClient,
		DriveClient:        driveClient,
		VolumeClient:       volumeClient,
		NodeClient:         nodeClient,
		InitRequestClient:  initRequestClient,
	}, nil
}
