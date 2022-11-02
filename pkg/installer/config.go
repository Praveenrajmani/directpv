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

package installer

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/minio/directpv/pkg/admin"
	"github.com/minio/directpv/pkg/consts"
	"github.com/minio/directpv/pkg/k8s"
	"github.com/minio/directpv/pkg/utils"
	corev1 "k8s.io/api/core/v1"
)

// CSI provisioner images
const (
	// quay.io/minio/csi-provisioner:v3.3.0
	CSIImageCSIProvisioner = "csi-provisioner:v3.3.0"

	// quay.io/minio/csi-node-driver-registrar:v2.6.0
	CSIImageNodeDriverRegistrar = "csi-node-driver-registrar:v2.6.0"

	// quay.io/minio/livenessprobe:v2.8.0
	CSIImageLivenessProbe = "livenessprobe:v2.8.0"
)

func defaultOnEmpty(s, d string) string {
	if s != "" {
		return s
	}
	return d
}

// Config defines the installer config
type Config struct {
	Identity string

	// ContainerImage properties
	ContainerImage    string
	ContainerOrg      string
	ContainerRegistry string

	// CSIImage properties
	CSIProvisionerImage      string
	NodeDriverRegistrarImage string
	LivenessProbeImage       string

	// Selectors and tolerations
	NodeSelector map[string]string
	Tolerations  []corev1.Toleration

	// Security profiles
	SeccompProfile  string
	ApparmorProfile string

	// dry-run properties
	DryRun bool

	// CRD uninstallation
	ForceRemove  bool
	UninstallCRD bool

	// Audit
	AuditFile *utils.SafeFile

	// Image pull secrets
	ImagePullSecrets []string

	Credential *admin.Credential

	// NodePort SVC setting for admin server
	DisableAdminService bool

	// Internal
	enablePodSecurityAdmission bool

	// Discard any messages to be printed on the stdout
	Quiet bool
}

type installer interface {
	Install(context.Context) error
	Uninstall(context.Context) error
}

func (c *Config) validate() error {
	c.Identity = k8s.SanitizeResourceName(c.Identity)
	if c.Identity == "" {
		return errors.New("identity cannot be empty")
	}
	return nil
}

func (c *Config) namespace() string {
	return c.Identity
}

func (c *Config) identity() string {
	return c.Identity
}

func (c *Config) getCSIProvisionerImage() string {
	return defaultOnEmpty(c.CSIProvisionerImage, CSIImageCSIProvisioner)
}

func (c *Config) getNodeDriverRegistrarImage() string {
	return defaultOnEmpty(c.NodeDriverRegistrarImage, CSIImageNodeDriverRegistrar)
}

func (c *Config) getLivenessProbeImage() string {
	return defaultOnEmpty(c.LivenessProbeImage, CSIImageLivenessProbe)
}

func (c *Config) csiDriverName() string {
	return c.identity()
}

func (c *Config) daemonsetName() string {
	return consts.NodeServerName
}

func (c *Config) deploymentName() string {
	return consts.ControllerServerName
}

func (c *Config) adminServerDeploymentName() string {
	return consts.AdminServerName
}

func (c *Config) getPSPName() string {
	return c.Identity
}

func (c *Config) getPSPClusterRoleBindingName() string {
	return k8s.SanitizeResourceName("psp-" + c.Identity)
}

func (c *Config) serviceAccountName() string {
	return c.Identity
}

func (c *Config) clusterRoleName() string {
	return c.Identity
}

func (c *Config) roleBindingName() string {
	return c.Identity
}

func (c *Config) storageClassName() string {
	return consts.StorageClassName
}

func (c *Config) driverIdentity() string {
	return c.Identity
}

func (c *Config) provisionerName() string {
	return c.Identity
}

func (c *Config) getImagePullSecrets() []corev1.LocalObjectReference {
	var localObjectReferences []corev1.LocalObjectReference
	for _, imagePullReferentName := range c.ImagePullSecrets {
		localObjectReferences = append(localObjectReferences, corev1.LocalObjectReference{
			Name: imagePullReferentName,
		})
	}
	return localObjectReferences
}

func (c *Config) postProc(obj interface{}, errFormat string, errArgs ...any) error {
	if obj != nil {
		if c.DryRun {
			yamlString, err := utils.ToYAML(obj)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n---\n", yamlString)
		}
		if c.AuditFile != nil {
			if err := utils.WriteObject(c.AuditFile, obj); err != nil {
				return err
			}
		}
	}
	if !c.DryRun && !c.Quiet {
		fmt.Fprintf(os.Stdout, errFormat+"\n", errArgs...)
	}
	return nil
}
