// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package objectstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
)

type objectStore struct {
	store  brtypes.SnapStore
	logger *logrus.Entry
}

// NewObjectStore creates a new object store with the given snapstore and logger.
func NewObjectStore(store brtypes.SnapStore, logger *logrus.Entry) brtypes.ObjectStore {
	return &objectStore{
		store:  store,
		logger: logger,
	}
}

// List lists all objects in the store.
func (os *objectStore) List() (brtypes.ObjectList, error) {
	snapList, err := os.store.List()
	if err != nil {
		return nil, &errors.SnapstoreError{
			Message: fmt.Sprintf("could not list snapshots in snapstore: %v", err),
		}
	}

	var objects brtypes.ObjectList
	for _, s := range snapList {
		if s.Kind == brtypes.SnapshotKindObject && !s.IsChunk {
			tokens := strings.Split(s.SnapName, "-")
			if len(tokens) != 4 {
				return nil, fmt.Errorf("invalid object snapshot name: %s", s.SnapName)
			}

			objects = append(objects, &brtypes.Object{
				Kind:      brtypes.ObjectKind(tokens[1]),
				Name:      tokens[2],
				CreatedOn: s.CreatedOn,
				Snapshot:  s,
			})
		}
	}

	return objects, nil
}

// Read reads the given object from the store.
func (os *objectStore) Read(obj *brtypes.Object, v interface{}) error {
	s := getObjectSnapshot(obj)
	rc, err := os.store.Fetch(*s)
	if err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("could not fetch object snapshot %+v from snapstore: %v", s, err),
		}
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("could not read object from snapstore: %v", err),
		}
	}

	if err := json.Unmarshal(buf.Bytes(), v); err != nil {
		return fmt.Errorf("could not unmarshal object from json: %w", err)
	}

	return nil
}

// Write writes the given object to the store.
func (os *objectStore) Write(obj *brtypes.Object, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("could not marshal object to json: %w", err)
	}

	rc := ioutil.NopCloser(bytes.NewReader(b))
	defer rc.Close()

	s := getObjectSnapshot(obj)
	if err := os.store.Save(*s, rc); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("could not save object snapshot %+v to snapstore: %v", s, err),
		}
	}

	return nil
}

// Delete deletes the given object from the store.
func (os *objectStore) Delete(obj *brtypes.Object) error {
	s := getObjectSnapshot(obj)
	if err := os.store.Delete(*s); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("could not delete object snapshot %+v from snapstore: %v", s, err),
		}
	}

	return nil
}

func getObjectSnapshot(obj *brtypes.Object) *brtypes.Snapshot {
	if obj.Snapshot != nil {
		return obj.Snapshot
	}
	return snapstore.NewObjectSnapshot(string(obj.Kind), obj.Name, obj.CreatedOn)
}
