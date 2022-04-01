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

package uevent

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/minio/directpv/pkg/sys"
)

func (l *listener) parseUEvent(buf []byte) (*deviceEvent, error) {
	eventMap, err := parse(buf)
	if err != nil {
		return nil, err
	}

	if eventMap["SUBSYSTEM"] != "block" {
		return nil, errNonDeviceEvent
	}

	eventAction := action(eventMap["ACTION"])
	switch eventAction {
	case Add, Change, Remove:
	default:
		return nil, fmt.Errorf("invalid device action: %s", eventAction)
	}

	path := eventMap["DEVPATH"]
	if path == "" {
		return nil, fmt.Errorf("invalid path: %s", path)
	}

	major, err := strconv.Atoi(eventMap["MAJOR"])
	if err != nil {
		return nil, err
	}

	minor, err := strconv.Atoi(eventMap["MINOR"])
	if err != nil {
		return nil, err
	}

	udevData, err := sys.MapToUdevData(eventMap)
	if err != nil {
		return nil, err
	}

	return &deviceEvent{
		created:  time.Now().UTC(),
		action:   eventAction,
		udevData: udevData,
		devPath:  "/dev/" + filepath.Base(path),
		major:    major,
		minor:    minor,
	}, nil
}

func parse(msg []byte) (map[string]string, error) {
	if !bytes.HasPrefix(msg, []byte(libudev)) {
		return nil, errors.New("libudev signature not found")
	}

	// magic number is stored in network byte order.
	if magic := binary.BigEndian.Uint32(msg[8:]); magic != libudevMagic {
		return nil, fmt.Errorf("libudev magic mismatch; expected: %v, got: %v", libudevMagic, magic)
	}

	offset := int(msg[16])
	if offset < 17 {
		return nil, fmt.Errorf("payload offset %v is not more than 17", offset)
	}
	if offset > len(msg) {
		return nil, fmt.Errorf("payload offset %v beyond message length %v", offset, len(msg))
	}

	fields := bytes.Split(msg[offset:], fieldDelimiter)
	event := map[string]string{}
	for _, field := range fields {
		if len(field) == 0 {
			continue
		}
		switch tokens := strings.SplitN(string(field), "=", 2); len(tokens) {
		case 1:
			event[tokens[0]] = ""
		case 2:
			event[tokens[0]] = tokens[1]
		}
	}
	return event, nil
}

func (l *listener) msgPeek(ctx context.Context) (int, []byte, error) {
	var n int
	var err error
	buf := make([]byte, os.Getpagesize())
	for {
		select {
		case <-ctx.Done():
			return n, nil, ctx.Err()
		case <-l.closeCh:
			return n, nil, errClosedListener
		default:
			if n, _, err = syscall.Recvfrom(l.sockfd, buf, syscall.MSG_PEEK); err != nil {
				return n, nil, err
			}
		}
		if n < len(buf) {
			break
		}
		buf = make([]byte, len(buf)+os.Getpagesize())
	}

	buf = buf[:n]

	return n, buf, err
}

func (l *listener) msgRead(buf []byte) error {
	if buf == nil {
		return errEmptyBuf
	}

	n, _, err := syscall.Recvfrom(l.sockfd, buf, 0)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return errShortRead
	}

	return nil
}

// ReadMsg allow to read an entire uevent msg
func (l *listener) readMsg(ctx context.Context) ([]byte, error) {
	for {
		n, buf, err := l.msgPeek(ctx)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			// continue peeking until a message has been written to the sock
			continue
		}
		if err = l.msgRead(buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
}
