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

package quota

import (
	"context"
	"fmt"
)

type FakeDriveQuotaer struct{}

func (fq *FakeDriveQuotaer) SetQuota(ctx context.Context, path, volumeID, blockFile string, quota FSQuota) error {
	if path == "" || volumeID == "" || blockFile == "" || quota.HardLimit <= int64(0) {
		return fmt.Errorf("Invalid arguments passed for SetQuota (%v, %v, %v, %v) ", path, volumeID, blockFile, quota)
	}
	return nil
}

func (fq *FakeDriveQuotaer) GetQuota(blockFile, volumeID string) (FSQuota, error) {
	if blockFile == "" || volumeID == "" {
		return FSQuota{}, fmt.Errorf("Invalid arguments passed for GetQuota (%v, %v) ", blockFile, volumeID)
	}
	return FSQuota{}, nil
}
