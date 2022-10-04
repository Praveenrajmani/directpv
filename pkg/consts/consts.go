// This file is part of MinIO
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

// AUTO GENERATED CODE. DO NOT EDIT.

package consts

const (
	// AppName denotes application/library/plugin/tool name
	AppName = "directpv"

	// AppPrettyName denotes application/library/plugin/tool pretty name
	AppPrettyName = "DirectPV"

	// AppCapsName denotes application/library/plugin/tool name in capital letters.
	AppCapsName = "DIRECTPV"

	// Group denotes group name.
	GroupName = AppName + ".min.io"

	// LatestAPIVersion denotes latest API version of drive/volume.
	LatestAPIVersion = "v1beta1"

	// Identity denotes identity value.
	Identity = AppName + "-min-io"

	// Namespace denotes the namespace where the app is installed
	Namespace = Identity

	// StorageClassName denotes storage class name.
	StorageClassName = Identity

	// DriveFinalizerDataProtection denotes data protection finalizer.
	DriveFinalizerDataProtection = GroupName + "/data-protection"

	// DriveFinalizerPrefix denotes prefix finalizer.
	DriveFinalizerPrefix = GroupName + ".volume/"

	// VolumeFinalizerPVProtection denotes PV protection finalizer.
	VolumeFinalizerPVProtection = GroupName + "/pv-protection"

	// VolumeFinalizerPurgeProtection denotes purge protection finalizer.
	VolumeFinalizerPurgeProtection = GroupName + "/purge-protection"

	// ControllerName is the name of the controller.
	ControllerName = AppName + "-controller"

	// DriverName is the driver name.
	DriverName = AppName + "-driver"

	// DriveKind is drive CRD kind.
	DriveKind = AppPrettyName + "Drive"

	// VolumeKind is volume CRD kind.
	VolumeKind = AppPrettyName + "Volume"

	// DriveResource is drive CRD resource.
	DriveResource = AppName + "drives"

	// VolumeResource is volume CRD resource.
	VolumeResource = AppName + "volumes"

	// AppRootDir is application root directory.
	AppRootDir = "/var/lib/" + AppName

	// SysFSDir is sysfs directory.
	SysFSDir = "/sys"

	// DevDir is /dev directory
	DevDir = "/dev"

	// UdevDataDir is Udev data directory.
	UdevDataDir = "/run/udev/data"

	// ProcFSDir is Udev data directory.
	ProcFSDir = "/proc"

	// MetricsPort is default metrics port.
	MetricsPort = 10443

	// ReadinessPort is default readiness port.
	ReadinessPort = 30443

	// ReadinessPath is default readiness path.
	ReadinessPath = "/ready"

	// FSLabel is filesystem label.
	FSLabel = AppCapsName

	// MountRootDir is mount root directory.
	MountRootDir = AppRootDir + "/mnt"

	// LegacyMountRootDir is the legacy mount dir
	LegacyMountRootDir = "/var/lib/direct-csi/mnt"

	// UnixCSIEndpoint is Unix CSI endpoint
	UnixCSIEndpoint = "unix:///csi/csi.sock"

	// APIServerContainerName is the name of the api server
	APIServerContainerName = "api-server"

	// NodeAPIServerContainerName is the name of the node api server
	NodeAPIServerContainerName = "node-api-server"

	// NodeAPIServerHLSVC denotes the name of the clusterIP service for the node API
	NodeAPIServerHLSVC = NodeAPIServerContainerName + "-hl"

	// APIPort is the default port for the api-server
	APIPortName        = "api-port"
	APIPort            = 40443
	APIServerCertsPath = "/tmp/apiserver/certs"

	// NodeAPIPort is the default port for the node-api-server
	NodeAPIPortName        = "node-api-port"
	NodeAPIPort            = 50443
	NodeAPIServerCAPath    = "/tmp/nodeapiserver/ca"
	NodeAPIServerCertsPath = "/tmp/nodeapiserver/certs"

	// key-pairs
	PrivateKeyFileName = "key.pem"
	PublicCertFileName = "cert.pem"
	CACertFileName     = "ca.crt"

	// Credentials
	CredentialsSecretName = AppName + "-creds"
	AccessKeyDataKey      = "accessKey"
	SecretKeyDataKey      = "secretKey"
	ConfigFileSuffix      = "." + AppName + "/config.json"
	AccessKeyEnv          = AppCapsName + "_ACCESS_KEY"
	SecretKeyEnv          = AppCapsName + "_SECRET_KEY"

	// Format
	APIServerEnv = AppCapsName + "_API_SERVER"
)
