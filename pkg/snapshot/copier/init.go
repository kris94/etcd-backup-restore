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
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	flag "github.com/spf13/pflag"
)

// NewConfig returns the copier config.
func NewConfig() *Config {
	sourceSnapstoreConfig := snapstore.NewSnapstoreConfig()
	sourceSnapstoreConfig.IsSource = true
	return &Config{
		SourceSnapstore: sourceSnapstoreConfig,
	}
}

// AddFlags adds the flags to flagset.
func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.SourceSnapstore.Provider, "source-storage-provider", c.SourceSnapstore.Provider, "source snapshot storage provider")
	fs.StringVar(&c.SourceSnapstore.Container, "source-store-container", c.SourceSnapstore.Container, "source container which will be used as snapstore")
	fs.StringVar(&c.SourceSnapstore.Prefix, "source-store-prefix", c.SourceSnapstore.Prefix, "source prefix or directory inside container under which snapstore is created")
}

// Validate validates the config.
func (c *Config) Validate() error {
	return nil
}

// Complete completes the config.
func (c *Config) CompleteWithSnapstoreConfig(other *snapstore.Config) {
	c.SourceSnapstore.CompleteWithOther(other)
}