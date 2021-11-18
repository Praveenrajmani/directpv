// This file is part of MinIO Direct CSI
// Copyright (c) 2021 MinIO, Inc.
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
	"context"
	"time"

	directcsi "github.com/minio/direct-csi/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/direct-csi/pkg/converter"

	scheme "github.com/minio/direct-csi/pkg/clientset/scheme"
	clientset "github.com/minio/direct-csi/pkg/clientset/typed/direct.csi.min.io/v1beta3"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

const (
	versionV1Alpha1 = "direct.csi.min.io/v1alpha1"
	versionV1Beta1  = "direct.csi.min.io/v1beta1"
	versionV1Beta2  = "direct.csi.min.io/v1beta2"
	versionV1Beta3  = "direct.csi.min.io/v1beta3"
)

//var _ rest.Interface = &directCSIDriveAdapter{}

type directCSIDriveAdapter struct {
	client rest.Interface
	gvk    *schema.GroupVersionKind
}

func DirectCSIDriveAdapter(r rest.Interface) clientset.DirectCSIDriveInterface {
	return &directCSIDriveAdapter{client: r}
}

// Get takes name of the directCSIDrive, and returns the corresponding directCSIDrive object, and an error if there is any.
func (c *directCSIDriveAdapter) Get(
	ctx context.Context,
	name string,
	options v1.GetOptions) (*directcsi.DirectCSIDrive, error) {
	intermediateResult := &unstructured.Unstructured{}
	err := c.client.Get().
		Resource("directcsidrives").
		Name(name).
		VersionedParams(&options, ParameterCodec).
		Do(ctx).
		Into(intermediateResult)

	finalResult := &unstructured.Unstructured{}
	err = converter.Migrate(intermediateResult, finalResult, schema.GroupVersion{
		Version: directcsi.Version,
		Group:   directcsi.Group,
	})
	if err != nil {
		return nil, err
	}

	unstructuredObject := finalResult.Object
	var directCSIDrive directcsi.DirectCSIDrive
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObject, directCSIDrive)
	if err != nil {
		return nil, err
	}
	return &directCSIDrive, nil

}

// List takes label and field selectors, and returns the list of DirectCSIDrives that match those selectors.
func (c *directCSIDriveAdapter) List(ctx context.Context, opts v1.ListOptions) (result *directcsi.DirectCSIDriveList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	intermediateResult := &unstructured.Unstructured{}
	err = c.client.Get().
		Resource("directcsidrives").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(intermediateResult)

	finalResult := &unstructured.Unstructured{}
	err = converter.Migrate(intermediateResult, finalResult, schema.GroupVersion{
		Version: directcsi.Version,
		Group:   directcsi.Group,
	})
	if err != nil {
		return nil, err
	}

	unstructuredObject := finalResult.Object
	var directCSIDriveList directcsi.DirectCSIDriveList
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObject, directCSIDriveList)
	if err != nil {
		return nil, err
	}
	return &directCSIDriveList, nil
}

// Watch returns a watch.Interface that watches the requested directCSIDrives.
func (c *directCSIDriveAdapter) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Resource("directcsidrives").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a directCSIDrive and creates it.  Returns the server's representation of the directCSIDrive, and an error, if there is any.
func (c *directCSIDriveAdapter) Create(ctx context.Context, directCSIDrive *directcsi.DirectCSIDrive, opts v1.CreateOptions) (result *directcsi.DirectCSIDrive, err error) {
	result = &directcsi.DirectCSIDrive{}
	err = c.client.Post().
		Resource("directcsidrives").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(directCSIDrive).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a directCSIDrive and updates it. Returns the server's representation of the directCSIDrive, and an error, if there is any.
func (c *directCSIDriveAdapter) Update(ctx context.Context, directCSIDrive *directcsi.DirectCSIDrive, opts v1.UpdateOptions) (result *directcsi.DirectCSIDrive, err error) {
	result = &directcsi.DirectCSIDrive{}
	err = c.client.Put().
		Resource("directcsidrives").
		Name(directCSIDrive.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(directCSIDrive).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *directCSIDriveAdapter) UpdateStatus(ctx context.Context, directCSIDrive *directcsi.DirectCSIDrive, opts v1.UpdateOptions) (result *directcsi.DirectCSIDrive, err error) {
	result = &directcsi.DirectCSIDrive{}
	err = c.client.Put().
		Resource("directcsidrives").
		Name(directCSIDrive.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(directCSIDrive).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the directCSIDrive and deletes it. Returns an error if one occurs.
func (c *directCSIDriveAdapter) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Resource("directcsidrives").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *directCSIDriveAdapter) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Resource("directcsidrives").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched directCSIDrive.
func (c *directCSIDriveAdapter) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *directcsi.DirectCSIDrive, err error) {
	result = &directcsi.DirectCSIDrive{}
	err = c.client.Patch(pt).
		Resource("directcsidrives").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}

// APIVersion returns the APIVersion this RESTClient is expected to use.
func (c *directCSIDriveAdapter) APIVersion() schema.GroupVersion {
	return c.gvk.GroupVersion()
}
