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

	"github.com/sirupsen/logrus"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
)

type objectStore struct {
	store  snapstore.SnapStore
	logger *logrus.Entry
}

func NewObjectStore(store snapstore.SnapStore, logger *logrus.Entry) ObjectStore {
	return &objectStore{
		store:  store,
		logger: logger,
	}
}

// List lists all objects in the store.
func (os *objectStore) List() (ObjectList, error) {
	snapList, err := os.store.List()
	if err != nil {
		return nil, &errors.SnapstoreError{
			Message: fmt.Sprintf("could not list snapshots in snapstore: %v", err),
		}
	}

	var objects ObjectList
	for _, s := range snapList {
		if s.Kind == snapstore.SnapshotKindObject && !s.IsChunk {
			tokens := strings.Split(s.SnapName, "-")
			if len(tokens) != 4 {
				return nil, fmt.Errorf("invalid object snapshot name: %s", s.SnapName)
			}

			objects = append(objects, &Object{
				Kind:      ObjectKind(tokens[1]),
				Name:      tokens[2],
				CreatedOn: s.CreatedOn,
			})
		}
	}

	return objects, nil
}

// Read reads the given object from the store.
func (os *objectStore) Read(obj *Object, v interface{}) error {
	s := snapstore.NewObjectSnapshot(string(obj.Kind), obj.Name, obj.CreatedOn)
	rc, err := os.store.Fetch(*s)
	if err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("could not fetch object snapshot from snapstore: %v", err),
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
func (os *objectStore) Write(obj *Object, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("could not marshal object to json: %w", err)
	}

	rc := ioutil.NopCloser(bytes.NewReader(b))
	defer rc.Close()

	s := snapstore.NewObjectSnapshot(string(obj.Kind), obj.Name, obj.CreatedOn)
	if err := os.store.Save(*s, rc); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("could not save object snapshot to snapstore: %v", err),
		}
	}

	return nil
}

// Delete deletes the given object from the store.
func (os *objectStore) Delete(obj *Object) error {
	s := snapstore.NewObjectSnapshot(string(obj.Kind), obj.Name, obj.CreatedOn)
	if err := os.store.Delete(*s); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("could not delete object snapshot from snapstore: %v", err),
		}
	}

	return nil
}
