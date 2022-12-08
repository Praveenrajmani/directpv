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

package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/v6/table"
	directpvtypes "github.com/minio/directpv/pkg/apis/directpv.min.io/types"
	"github.com/minio/directpv/pkg/client"
	"github.com/minio/directpv/pkg/consts"
	"github.com/minio/directpv/pkg/types"
	"github.com/minio/directpv/pkg/utils"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var errInitFailed = errors.New("init failed")

var initCmd = &cobra.Command{
	Use:           "init drives.yaml",
	Short:         "Initialize the drives",
	SilenceUsage:  true,
	SilenceErrors: true,
	Example: strings.ReplaceAll(
		`# Initialize the drives
$ kubectl {PLUGIN_NAME} init drives.yaml`,
		`{PLUGIN_NAME}`,
		consts.AppName,
	),
	Run: func(c *cobra.Command, args []string) {
		if err := validateAdminServerConfigArgs(); err != nil {
			utils.Eprintf(quietFlag, true, "%v\n", err)
			os.Exit(-1)
		}

		switch len(args) {
		case 1:
		case 0:
			utils.Eprintf(quietFlag, true, "Please provide the input file. Check `--help` for usage.\n")
			os.Exit(-1)
		default:
			utils.Eprintf(quietFlag, true, "Too many input args. Check `--help` for usage.\n")
			os.Exit(-1)
		}

		input := getInput(color.HiRedString("Initializing the drives will permanently erase existing data. Do you really want to continue (Yes|No)? "))
		if input != "Yes" {
			utils.Eprintf(quietFlag, false, "Aborting...\n")
			os.Exit(1)
		}

		initMain(c.Context(), args[0])
	},
}

func init() {
	addAdminServerFlag(initCmd)
}

func toInitRequest(config *InitConfig) types.InitRequest {
	request := map[directpvtypes.NodeID][]types.InitDevice{}
	for _, node := range config.Nodes {
		initDevices := []types.InitDevice{}
		for _, device := range node.Drives {
			initDevices = append(initDevices, types.InitDevice{
				ID:         device.ID,
				Name:       device.Name,
				MajorMinor: device.MajorMinor,
				Force:      device.FS != "",
			})
		}
		if len(initDevices) > 0 {
			request[node.Name] = initDevices
		}
	}

	return types.InitRequest{
		TypeMeta:   types.NewInitRequestTypeMeta(),
		ObjectMeta: metav1.ObjectMeta{Name: uuid.NewString()},
		Status:     types.InitRequestStatus{Request: request},
	}
}

func initDevices(ctx context.Context, req types.InitRequest) error {
	if len(req.Status.Request) == 0 {
		utils.Eprintf(false, false, "%v\n", color.HiYellowString("No drives are available to init"))
		return errInitFailed
	}

	if _, err := client.InitRequestClient().Create(ctx, &req, metav1.CreateOptions{}); err != nil {
		return err
	}

	// FIXME: watch newReq changes and show results.
	time.Sleep(1 * time.Minute)
	updated, err := client.InitRequestClient().Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	writer := newTableWriter(
		table.Row{
			"NODE",
			"DRIVE",
			"MESSAGE",
		},
		[]table.SortBy{
			{
				Name: "MESSAGE",
				Mode: table.Asc,
			},
			{
				Name: "NODE",
				Mode: table.Asc,
			},
			{
				Name: "DRIVE",
				Mode: table.Asc,
			},
		},
		false,
	)

	errs := map[string]string{}
	for node, result := range updated.Spec.Response {
		// FIXME: use result.Completed which denotes whether specified node processed the request or not.
		if result.Error != "" {
			errs[string(node)] = result.Error
			continue
		}

		for _, device := range result.Devices {
			msg := "Success"
			if device.Error != "" {
				msg = "Failed; " + device.Error
			}

			writer.AppendRow(
				[]interface{}{
					node,
					device.Name,
					msg,
				},
			)
		}
	}

	writer.Render()

	if len(errs) != 0 {
		for node, err := range errs {
			utils.Eprintf(quietFlag, true, "%v: %v\n", node, err)
		}

		return errInitFailed
	}

	return nil
}

func readInitConfig(inputFile string) (*InitConfig, error) {
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseInitConfig(f)
}

func initMain(ctx context.Context, inputFile string) {
	initConfig, err := readInitConfig(inputFile)
	if err != nil {
		utils.Eprintf(quietFlag, true, "unable to read the input file; %v", err.Error())
		os.Exit(1)
	}
	err = initDevices(ctx, toInitRequest(initConfig))
	if err != nil {
		if !errors.Is(err, errInitFailed) {
			utils.Eprintf(quietFlag, true, "%v\n", err)
		}
		os.Exit(1)
	}
}
