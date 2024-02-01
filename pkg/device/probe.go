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

package device

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"

	directpvtypes "github.com/minio/directpv/pkg/apis/directpv.min.io/types"
	"github.com/minio/directpv/pkg/consts"
	"github.com/minio/directpv/pkg/types"
	"github.com/minio/directpv/pkg/utils"
	"github.com/minio/directpv/pkg/xfs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

const minSupportedDeviceSize = 512 * 1024 * 1024 // 512 MiB

// ProbedDevice represents the device probed.
type ProbedDevice struct {
	Device
	FSUUID        string
	Label         string
	TotalCapacity int64
	FreeCapacity  int64
}

// Device is a block device information.
type Device struct {
	Name        string            `json:"name"`        // Read from /sys/dev/block/<Major:Minor>/uevent
	MajorMinor  string            `json:"majorMinor"`  // Read from /run/udev/data
	Size        uint64            `json:"size"`        // Read from /sys/class/block/<NAME>/size
	Hidden      bool              `json:"hidden"`      // Read from /sys/class/block/<NAME>/hidden
	Removable   bool              `json:"removable"`   // Read from /sys/class/block/<NAME>/removable
	ReadOnly    bool              `json:"readOnly"`    // Read from /sys/class/block/<NAME>/ro
	Partitioned bool              `json:"partitioned"` // Read from /sys/block/<NAME>/<NAME>*
	Holders     []string          `json:"holders"`     // Read from /sys/class/block/<NAME>/holders
	MountPoints []string          `json:"mountPoints"` // Read from /proc/1/mountinfo or /proc/mounts
	SwapOn      bool              `json:"swapOn"`      // Read from /proc/swaps
	CDROM       bool              `json:"cdrom"`       // Read from /proc/sys/dev/cdrom/info
	DMName      string            `json:"dmName"`      // Read from /sys/class/block/<NAME>/dm/name
	udevData    map[string]string // Read from /run/udev/data/b<Major:Minor>
}

// ID generates an unique ID by hashing the properties of the Device.
func (d Device) ID(nodeID directpvtypes.NodeID) string {
	sort.Strings(d.Holders)
	sort.Strings(d.MountPoints)

	deviceMap := map[string]string{
		"node":        string(nodeID),
		"name":        d.Name,
		"majorminor":  d.MajorMinor,
		"size":        fmt.Sprintf("%v", d.Size),
		"hidden":      fmt.Sprintf("%v", d.Hidden),
		"removable":   fmt.Sprintf("%v", d.Removable),
		"readonly":    fmt.Sprintf("%v", d.ReadOnly),
		"partitioned": fmt.Sprintf("%v", d.Partitioned),
		"holders":     strings.Join(d.Holders, ","),
		"mountpoints": strings.Join(d.MountPoints, ","),
		"swapon":      fmt.Sprintf("%v", d.SwapOn),
		"cdrom":       fmt.Sprintf("%v", d.CDROM),
		"dmname":      d.DMName,
		"udevdata":    strings.Join(toSlice(d.udevData, "="), ";"),
	}

	stringToHash := strings.Join(toSlice(deviceMap, ":"), "\n")
	h := sha256.Sum256([]byte(stringToHash))
	return d.MajorMinor + "$" + base64.StdEncoding.EncodeToString(h[:])
}

// Make returns device make information.
func (d Device) Make() string {
	var tokens []string

	if d.DMName != "" {
		tokens = append(tokens, d.DMName)
	}

	if d.udevData["E:ID_VENDOR"] != "" {
		tokens = append(tokens, d.udevData["E:ID_VENDOR"])
	}

	if d.udevData["E:ID_MODEL"] != "" {
		tokens = append(tokens, d.udevData["E:ID_MODEL"])
	}

	if number, found := d.udevData["E:ID_PART_ENTRY_NUMBER"]; found {
		tokens = append(tokens, fmt.Sprintf("(Part %v)", number))
	}

	return strings.Join(tokens, " ")
}

// FSType returns filesystem type.
func (d Device) FSType() string {
	return d.udevData["E:ID_FS_TYPE"]
}

// FSUUID returns the filesystem UUID.
func (d Device) FSUUID() string {
	return d.udevData["E:ID_FS_UUID"]
}

// deniedReason returns the reason if the device is denied for initialization.
func (d Device) deniedReason(findDriveByFSUUID func(string) error) string {
	var reasons []string

	if d.Size < minSupportedDeviceSize {
		reasons = append(reasons, "Too small")
	}

	if d.Hidden {
		reasons = append(reasons, "Hidden")
	}

	if d.ReadOnly {
		reasons = append(reasons, "Read only")
	}

	if d.Partitioned {
		reasons = append(reasons, "Partitioned")
	}

	if len(d.Holders) != 0 {
		reasons = append(reasons, "Held by other device")
	}

	if len(d.MountPoints) != 0 {
		reasons = append(reasons, "Mounted")
	}

	if d.SwapOn {
		reasons = append(reasons, "Swap")
	}

	if d.CDROM {
		reasons = append(reasons, "CDROM")
	}

	if d.FSType() == "xfs" && d.FSUUID() != "" {
		if err := findDriveByFSUUID(d.FSUUID()); err != nil {
			if !apierrors.IsNotFound(err) {
				reasons = append(reasons, "internal error; "+err.Error())
			}
		} else {
			reasons = append(reasons, "Used by "+consts.AppPrettyName)
		}
	}

	var reason string
	if len(reasons) != 0 {
		reason = strings.Join(reasons, "; ")
	}

	return reason
}

// ToNodeDevice constructs the NodeDevice object from Device info.
func (d Device) ToNodeDevice(nodeID directpvtypes.NodeID, findDriveByFSUUID func(string) error) types.Device {
	return types.Device{
		Name:         d.Name,
		ID:           d.ID(nodeID),
		MajorMinor:   d.MajorMinor,
		Size:         d.Size,
		Make:         d.Make(),
		FSType:       d.FSType(),
		FSUUID:       d.FSUUID(),
		DeniedReason: d.deniedReason(findDriveByFSUUID),
	}
}

// Probe returns block devices from udev.
func Probe() ([]Device, error) {
	return probe()
}

// ProbeDevices returns block devices from udev.
func ProbeDevices(majorMinor ...string) ([]Device, error) {
	return probeDevices(majorMinor...)
}

// ProbeDeviceMap probes and returns block devices.
func ProbeDeviceMap() (map[string][]ProbedDevice, error) {
	devices, err := Probe()
	if err != nil {
		return nil, err
	}

	deviceMap := map[string][]ProbedDevice{}
	for _, dev := range devices {
		if dev.Hidden || dev.Partitioned || len(dev.Holders) != 0 || dev.SwapOn || dev.CDROM || dev.Size == 0 {
			continue
		}

		fsuuid, label, totalCapacity, freeCapacity, err := xfs.Probe(utils.AddDevPrefix(dev.Name))
		if err != nil {
			if !errors.Is(err, xfs.ErrFSNotFound) {
				klog.ErrorS(err, "unable to probe XFS filesystem", "Device", dev.Name)
			}
			continue
		}

		deviceMap[fsuuid] = append(deviceMap[fsuuid], ProbedDevice{
			Device:        dev,
			FSUUID:        fsuuid,
			Label:         label,
			TotalCapacity: int64(totalCapacity),
			FreeCapacity:  int64(freeCapacity),
		})
	}

	return deviceMap, nil
}
