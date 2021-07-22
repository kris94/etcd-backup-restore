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
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
)

// GetSourceAndDestinationStores returns the source and destination stores for the given copier and store configs.
func GetSourceAndDestinationStores(config *brtypes.CopierConfig, destSnapStoreConfig *brtypes.SnapstoreConfig) (brtypes.SnapStore, brtypes.SnapStore, error) {
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

// NewCopierConfig creates a new copier config.
func NewCopierConfig() *brtypes.CopierConfig {
	sourceSnapstoreConfig := snapstore.NewSnapstoreConfig()
	sourceSnapstoreConfig.IsSource = true
	return &brtypes.CopierConfig{
		SourceSnapstore: sourceSnapstoreConfig,
	}
}

// Copier can be used to copy backups from a source to a destination store.
type Copier struct {
	logger          *logrus.Entry
	sourceSnapStore brtypes.SnapStore
	snapStore       brtypes.SnapStore
}

// NewCopier creates a new copier.
func NewCopier(sourceSnapStore brtypes.SnapStore, snapStore brtypes.SnapStore, logger *logrus.Entry) *Copier {
	return &Copier{
		logger:          logger.WithField("actor", "copier"),
		sourceSnapStore: sourceSnapStore,
		snapStore:       snapStore,
	}
}

// Run runs the copy command.
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

// HandleCopyOperation gets and handles a CopyOperation object in the source store.
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

	if copyOp.Status == brtypes.OperationStatusDone {
		c.logger.Info("Operation is already Done, nothing to do")
		return nil
	}

	if copyOp.Status == brtypes.OperationStatusInitial {
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
	copyOp.Status = brtypes.OperationStatusDone
	if err := SetCopyOperation(os, obj, copyOp); err != nil {
		return fmt.Errorf("could not set copy operation: %v", err)
	}
	c.logger.Info("Copy operation status set to Done")

	return nil
}

// WaitForCopyOperationReady waits for a copy operation in the given object store to become ready.
// When this is the case, it returns the copy operation and its corresponding object.
func WaitForCopyOperationReady(timer *time.Timer, os brtypes.ObjectStore) (*brtypes.Object, *brtypes.CopyOperation, error) {
	for {
		select {
		case <-timer.C:
			var obj *brtypes.Object
			var copyOp *brtypes.CopyOperation
			var err error
			if obj, copyOp, err = GetCopyOperation(os); err != nil {
				return nil, nil, err
			}
			if copyOp != nil && copyOp.Status == brtypes.OperationStatusReady {
				return obj, copyOp, nil
			}
			timer.Reset(15 * time.Second)
		}
	}
}

// InitializeCopyOperation initializes a new copy operation and its corresponding object.
func InitializeCopyOperation() (*brtypes.Object, *brtypes.CopyOperation) {
	now := time.Now().UTC()
	copyOp := &brtypes.CopyOperation{
		Status: brtypes.OperationStatusInitial,
	}
	obj := &brtypes.Object{
		Kind:      brtypes.ObjectKindCopyOperation,
		Name:      "test",
		CreatedOn: now,
	}
	return obj, copyOp
}

// GetCopyOperation reads a copy operation and its corresponding object from the given object store.
func GetCopyOperation(os brtypes.ObjectStore) (*brtypes.Object, *brtypes.CopyOperation, error) {
	objects, err := os.List()
	if err != nil {
		return nil, nil, err
	}

	if len(objects) == 0 {
		return nil, nil, nil
	}
	if len(objects) > 1 {
		return nil, nil, fmt.Errorf("multiple objects found")
	}
	if objects[0].Kind != brtypes.ObjectKindCopyOperation {
		return nil, nil, fmt.Errorf("found object of kind different from CopyOperation")
	}

	copyOp := &brtypes.CopyOperation{}
	if err := os.Read(objects[0], copyOp); err != nil {
		return nil, nil, err
	}
	return objects[0], copyOp, nil
}

// SetCopyOperation writes the given copy operation and its corresponding object to the given object store.
func SetCopyOperation(os brtypes.ObjectStore, obj *brtypes.Object, copyOp *brtypes.CopyOperation) error {
	if err := os.Write(obj, copyOp); err != nil {
		return err
	}
	return nil
}
