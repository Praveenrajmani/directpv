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

package k8s

import (
	"fmt"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	// support gcp, azure, and oidc client auth
	_ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

// MaxThreadCount is maximum thread count.
const MaxThreadCount = 200

// Client represents the kubernetes client set.
type Client struct {
	KubeConfig          *rest.Config
	KubeClient          kubernetes.Interface
	APIextensionsClient apiextensions.ApiextensionsV1Interface
	CRDClient           apiextensions.CustomResourceDefinitionInterface
	DiscoveryClient     discovery.DiscoveryInterface
}

// NewClient initializes and returns k8s client.
func NewClient() (*Client, error) {
	if fakeMode {
		return NewFakeClient()
	}
	kubeConfig, err := GetKubeConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to get kubernetes configuration; %v", err)
	}
	kubeConfig.WarningHandler = rest.NoWarnings{}
	return NewClientWithConfig(kubeConfig)
}

// NewClientWithConfig initializes the client with the provided kube config.
func NewClientWithConfig(kubeConfig *rest.Config) (*Client, error) {
	if fakeMode {
		return NewFakeClient()
	}
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new kubernetes client interface; %v", err)
	}
	apiextensionsClient, err := apiextensions.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new API extensions client interface; %v", err)
	}
	crdClient := apiextensionsClient.CustomResourceDefinitions()
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create new discovery client interface; %v", err)
	}
	return &Client{
		KubeConfig:          kubeConfig,
		KubeClient:          kubeClient,
		APIextensionsClient: apiextensionsClient,
		CRDClient:           crdClient,
		DiscoveryClient:     discoveryClient,
	}, nil
}
