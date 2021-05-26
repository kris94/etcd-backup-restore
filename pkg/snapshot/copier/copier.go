// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copier

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/objectstore"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	"github.com/sirupsen/logrus"
)

func GetSourceAndDestinationStores(config *Config, destSnapStoreConfig *snapstore.Config) (snapstore.SnapStore, snapstore.SnapStore, error) {
	sourceSnapStore, err := snapstore.GetSnapstore(config.SourceSnapstore)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get source snapstore: %v", err)
	}

	destSnapStore, err := snapstore.GetSnapstore(destSnapStoreConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get destination snapstore: %v", err)
	}

	return sourceSnapStore, destSnapStore, nil
}

// NewCopier returns a new copier
func NewCopier(sourceSnapStore snapstore.SnapStore, snapStore snapstore.SnapStore, logger *logrus.Entry) *Copier {
	return &Copier{
		logger:          logger.WithField("actor", "copier"),
		sourceSnapStore: sourceSnapStore,
		snapStore:       snapStore,
	}
}

// Run runs the copy command
func (c *Copier) Run() error {
	backups, err := miscellaneous.GetAllBackups(c.sourceSnapStore)
	if err != nil {
		return fmt.Errorf("failed to get backups from source: %v", err)
	}
	if backups == nil {
		return fmt.Errorf("no snapshots found, will do nothing")
	}

	for _, backup := range backups {
		rc, err := c.sourceSnapStore.Fetch(*backup.FullSnapshot)
		if err != nil {
			return fmt.Errorf("failed to fetch full snapshot from source: %v", err)
		}

		if err := c.snapStore.Save(*backup.FullSnapshot, rc); err != nil {
			return fmt.Errorf("failed to save full snapshot to destination: %v", err)
		}

		for _, deltaSnap := range backup.DeltaSnapshotList {
			rc, err := c.sourceSnapStore.Fetch(*deltaSnap)
			if err != nil {
				return fmt.Errorf("failed to fetch delta snapshot from source: %v", err)
			}

			if err := c.snapStore.Save(*deltaSnap, rc); err != nil {
				return fmt.Errorf("failed to save delta snapshot to destination: %v", err)
			}
		}
	}
	return nil
}

func (c *Copier) HandleCopyOperation() error {
	os := objectstore.NewObjectStore(c.sourceSnapStore, c.logger)

	obj, copyOp, err := GetCopyOperation(os)
	if err != nil {
		return fmt.Errorf("could not get copy operation: %v", err)
	}

	if copyOp == nil {
		c.logger.Info("Initiating copy operation...")
		obj, copyOp = InitializeCopyOperation()
		if err := SetCopyOperation(os, obj, copyOp); err != nil {
			return fmt.Errorf("could not set copy operation: %v", err)
		}
		c.logger.Info("Copy operation initiated")
	}

	if copyOp.Status == objectstore.OperationStatusDone {
		c.logger.Info("Operation is already Done, nothing to do")
		return nil
	}

	if copyOp.Status == objectstore.OperationStatusInitial {
		c.logger.Info("Waiting for copy operation to become Ready...")
		obj, copyOp, err = WaitForCopyOperationReady(time.NewTimer(15*time.Second), os)
		if err != nil {
			return fmt.Errorf("could not wait for copy operation to become Ready: %v", err)
		}
		c.logger.Info("Copy operation became Ready")
	}

	c.logger.Info("Copying backups ...")
	if err := c.Run(); err != nil {
		return fmt.Errorf("could not copy backups: %v", err)
	}
	c.logger.Info("Backups copied")

	c.logger.Info("Setting copy operation status to Done...")
	copyOp.Status = objectstore.OperationStatusDone
	if err := SetCopyOperation(os, obj, copyOp); err != nil {
		return fmt.Errorf("could not set copy operation: %v", err)
	}
	c.logger.Info("Copy operation status set to Done")

	return nil
}

func WaitForCopyOperationReady(timer *time.Timer, os objectstore.ObjectStore) (*objectstore.Object, *objectstore.CopyOperation, error) {
	for {
		select {
		case <-timer.C:
			var obj *objectstore.Object
			var copyOp *objectstore.CopyOperation
			var err error
			if obj, copyOp, err = GetCopyOperation(os); err != nil {
				return nil, nil, err
			}
			if copyOp != nil && copyOp.Status == objectstore.OperationStatusReady {
				return obj, copyOp, nil
			}
			timer.Reset(15 * time.Second)
		}
	}
}

func InitializeCopyOperation() (*objectstore.Object, *objectstore.CopyOperation) {
	now := time.Now().UTC()
	copyOp := &objectstore.CopyOperation{
		Status: objectstore.OperationStatusInitial,
	}
	obj := &objectstore.Object{
		Kind:      objectstore.ObjectKindCopyOperation,
		Name:      "test",
		CreatedOn: now,
	}
	return obj, copyOp
}

func GetCopyOperation(os objectstore.ObjectStore) (*objectstore.Object, *objectstore.CopyOperation, error) {
	// If there is a blocker, fail
	_, blocker, err := snapshotter.GetBlocker(os)
	if err != nil {
		return nil, nil, err
	}
	if blocker != nil {
		return nil, nil, fmt.Errorf("blocker found, failing GetCopyOperation")
	}

	objects, err := os.List()
	if err != nil {
		return nil, nil, err
	}

	for _, object := range objects {
		if object.Kind == objectstore.ObjectKindCopyOperation {
			copyOp := &objectstore.CopyOperation{}
			if err := os.Read(object, copyOp); err != nil {
				return nil, nil, err
			}
			return object, copyOp, nil
		}
	}

	return nil, nil, nil
}

func SetCopyOperation(os objectstore.ObjectStore, obj *objectstore.Object, copyOp *objectstore.CopyOperation) error {
	// If there is a blocker, fail
	_, blocker, err := snapshotter.GetBlocker(os)
	if err != nil {
		return err
	}
	if blocker != nil {
		return fmt.Errorf("blocker found, failing SetCopyOperation")
	}

	if err := os.Write(obj, copyOp); err != nil {
		return err
	}
	return nil
}
