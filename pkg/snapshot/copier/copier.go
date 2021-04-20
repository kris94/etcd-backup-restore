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

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	"github.com/sirupsen/logrus"
)

func GetSourceAndDestinationStores(config *Config, destSnapStoreConfig *snapstore.Config) (snapstore.SnapStore, snapstore.SnapStore, error) {
	sourceSnapStore, err := snapstore.GetSnapstore(config.SourceSnapstore)
	if err != nil {
		return nil, nil, fmt.Errorf("Could not retrieve source snapstore: %v", err)
	}

	destSnapStore, err := snapstore.GetSnapstore(destSnapStoreConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("Could not retrieve destination snapstore: %v", err)
	}

	return sourceSnapStore, destSnapStore, nil
}

// NewCopier returns a new copier
func NewCopier(sourceSnapStore snapstore.SnapStore, snapStore snapstore.SnapStore, logger *logrus.Entry) *Copier {
	return &Copier{
		logger:          logger.WithField("actor", "snapshotter"),
		sourceSnapStore: sourceSnapStore,
		snapStore:       snapStore,
	}
}

// Run runs the copy command
func (c *Copier) Run() error {
	backups, err := miscellaneous.GetAllBackups(c.sourceSnapStore)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot: %v", err)
	}
	if backups == nil {
		return fmt.Errorf("No snapshot found. Will do nothing.")
	}

	for _, backup := range backups {
		rc, err := c.sourceSnapStore.Fetch(*backup.FullSnapshot)
		if err != nil {
			return fmt.Errorf("failed to get readerCloser form baseSnapshot")
		}

		if err := c.snapStore.Save(*backup.FullSnapshot, rc); err != nil {
			return fmt.Errorf("failed to save snapshot to destination")
		}

		for _, deltaSnap := range backup.DeltaSnapshotList {
			rc, err := c.sourceSnapStore.Fetch(*deltaSnap)
			if err != nil {
				return fmt.Errorf("failed to get readerCloser form baseSnapshot")
			}

			if err := c.snapStore.Save(*deltaSnap, rc); err != nil {
				return fmt.Errorf("failed to save snapshot to destination")
			}
		}
	}
	return nil
}
