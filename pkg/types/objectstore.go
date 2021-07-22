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

package types

import (
	"time"
)

// ObjectStore is a generic object store.
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

// ObjectKind is a type for the kind of an object stored in an object store.
type ObjectKind string

const (
	// ObjectKindCopyOperation is a constant for the kind of a CopyOperation object.
	ObjectKindCopyOperation ObjectKind = "CopyOperation"
)

// Object is an object stored in an object store.
type Object struct {
	// Kind is the object kind.
	Kind ObjectKind `json:"kind"`
	// Name is the object name.
	Name string `json:"name"`
	// CreatedOn is the object creation timestamp.
	CreatedOn time.Time `json:"createdOn"`
	// Snapshot is the underlying snapshot.
	Snapshot *Snapshot `json:"snapshot"`
}

// ObjectList is a list of objects stored in an object store.
type ObjectList []*Object

// OperationStatus is type for the status of a copy operation.
type OperationStatus string

const (
	// OperationStatusInitial is a constant for the Initial status of a copy operation.
	OperationStatusInitial OperationStatus = "Initial"
	// OperationStatusReady is a constant for the Ready status of a copy operation.
	OperationStatusReady OperationStatus = "Ready"
	// OperationStatusDone is a constant for the Done status of a copy operation.
	OperationStatusDone OperationStatus = "Done"
)

// CopyOperation is a copy operation from a source to a destination store.
type CopyOperation struct {
	// Status is the copy operation status.
	Status OperationStatus `json:"status"`
}
