// This file is part of MinIO Kubernetes Cloud
// Copyright (c) 2020 MinIO, Inc.
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

package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/minio/direct-csi/pkg/centralcontroller"
	//	"github.com/minio/direct-csi/pkg/driver"

	_ "github.com/golang/glog"
)

const VERSION = "DEVELOPMENT"

// flags
var (
	identity   = "direct.csi.min.io"
	nodeID     = ""
	rack       = "default"
	zone       = "default"
	region     = "default"
	endpoint   = "unix://csi/csi.sock"
	leaderLock = ""

	ctx context.Context
)

func init() {
	viper.AutomaticEnv()

	directCSICmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	flag.Set("logtostderr", "true")

	strFlag := func(c *cobra.Command, ptr *string, name string, short string, dfault string, desc string) {
		c.PersistentFlags().
			StringVarP(ptr, name, short, dfault, desc)
	}
	strFlag(directCSICmd, &identity, "identity", "i", identity, "unique name for this CSI driver")

	strFlag(directCSIDriverCmd, &endpoint, "endpoint", "e", endpoint, "endpoint at which direct-csi is listening")
	strFlag(directCSIDriverCmd, &nodeID, "node-id", "n", nodeID, "identity of the node in which direct-csi is running")
	strFlag(directCSIDriverCmd, &rack, "rack", "", rack, "identity of the rack in which this direct-csi is running")
	strFlag(directCSIDriverCmd, &zone, "zone", "", zone, "identity of the zone in which this direct-csi is running")
	strFlag(directCSIDriverCmd, &region, "region", "", region, "identity of the region in which this direct-csi is running")

	strFlag(directCSIControllerCmd, &leaderLock, "leader-lock", "l", identity, "name of the lock used for leader election (defaults to identity)")

	hideFlag := func(name string) {
		directCSICmd.PersistentFlags().MarkHidden(name)
	}
	hideFlag("alsologtostderr")
	hideFlag("log_backtrace_at")
	hideFlag("log_dir")
	hideFlag("logtostderr")
	hideFlag("master")
	hideFlag("stderrthreshold")
	hideFlag("vmodule")

	// suppress the incorrect prefix in glog output
	flag.CommandLine.Parse([]string{})
	viper.BindPFlags(directCSICmd.PersistentFlags())

	var cancel context.CancelFunc

	ctx, cancel = context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSEGV)

	go func() {
		s := <-sigs
		cancel()
		panic(fmt.Sprintf("%s %s", s.String(), "Signal received. Exiting"))
	}()

	directCSICmd.AddCommand(directCSICentralControllerCmd, directCSIControllerCmd, directCSIDriverCmd)
}

var directCSICmd = &cobra.Command{
	Use:           "direct-csi",
	Short:         "CSI driver for dynamically provisioning local volumes",
	Long:          "",
	SilenceErrors: true,
	Version:       VERSION,
}

var directCSICentralControllerCmd = &cobra.Command{
	Use:           "central-controller",
	Short:         "run the central-controller for managing resources related to directCSI driver",
	Long:          "",
	SilenceErrors: true,
	RunE: func(c *cobra.Command, args []string) error {
		return centralControllerManager(args)
	},
}

var directCSIControllerCmd = &cobra.Command{
	Use:           "controller",
	Short:         "run the controller for managing resources related to directCSI driver",
	Long:          "",
	SilenceErrors: true,
	RunE: func(c *cobra.Command, args []string) error {
		return controllerManager(args)
	},
}

var directCSIDriverCmd = &cobra.Command{
	Use:           "driver",
	Short:         "run the driver for managing resources related to directCSI driver",
	Long:          "",
	SilenceErrors: true,
	RunE: func(c *cobra.Command, args []string) error {
		return driverManager(args)
	},
}

func driverManager(args []string) error {
	return nil
	// d := driver.Driver{
	// 	Identity: identity,
	// 	Rack:     rack,
	// 	Zone:     zone,
	// 	Region:   region,
	// 	Endpoint: endpoint,
	// 	NodeId:   nodeID,
	// }

	// return d.Run(ctx)
}

func centralControllerManager(args []string) error {
	c := centralcontroller.Controller{
		Identity:      identity,
		LeaderLock:    leaderLock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		ResyncPeriod:  30 * time.Second,
	}

	return c.Run(ctx)
}

func controllerManager(args []string) error {
	// c := controller.Controller{
	// 	Identity:      identity,
	// 	LeaderLock:    leaderLock,
	// 	LeaseDuration: 15 * time.Second,
	// 	RenewDeadline: 10 * time.Second,
	// 	RetryPeriod:   2 * time.Second,
	// 	ResyncPeriod:  30 * time.Second,
	// }

	// return c.Run(ctx)
	return nil
}


func Run() error {
	return directCSICmd.Execute()
}
