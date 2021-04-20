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

package objectstore

import (
	"time"
)

type ObjectStore interface {
	// List lists all objects in the store.
	List() (ObjectList, error)
	// Read reads the given object from the store.
	Read(*Object, interface{}) error
	// Write writes the given object to the store.
	Write(*Object, interface{}) error
	// Delete deletes the given object from the store.
	Delete(*Object) error
}

type ObjectKind string

const (
	ObjectKindCopyOperation ObjectKind = "CopyOperation"
)

type Object struct {
	Kind      ObjectKind `json:"kind"`
	Name      string     `json:"name"`
	CreatedOn time.Time  `json:"createdOn"`
}

type ObjectList []*Object

type OperationStatus string

const (
	OperationStatusInitial OperationStatus = "Initial"
	OperationStatusReady   OperationStatus = "Ready"
	OperationStatusDone    OperationStatus = "Done"
)

type CopyOperation struct {
	Source    bool            `json:"source"`
	Owner     string          `json:"owner"`
	Initiated time.Time       `json:"initiated"`
	Status    OperationStatus `json:"status"`
}
