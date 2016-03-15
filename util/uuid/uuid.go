// Copyright © 2016 Abcum Ltd
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

package uuid

import (
	"github.com/satori/go.uuid"
)

var (
	NamespaceDNS  = uuid.NamespaceDNS
	NamespaceURL  = uuid.NamespaceURL
	NamespaceOID  = uuid.NamespaceOID
	NamespaceX500 = uuid.NamespaceX500
)

type UUID struct {
	uuid.UUID
}

// NewV1 returns a new UUID (Version 1) based on current timestamp and MAC address.
func NewV1() *UUID {
	return &UUID{uuid.NewV1()}
}

// NewV2 returns a new DCE Security UUID (Version 2) based on POSIX UID/GID.
func NewV2(domain byte) *UUID {
	return &UUID{uuid.NewV2(domain)}
}

// NewV3 returns a new UUID (Version 3) based on MD5 hash of namespace UUID and name.
func NewV3(ns uuid.UUID, name string) *UUID {
	return &UUID{uuid.NewV3(ns, name)}
}

// NewV4 returns a new UUID (Version 4) using 16 random bytes or panics.
func NewV4() *UUID {
	return &UUID{uuid.NewV4()}
}

// NewV5 returns a new UUID (Version 5) based on SHA-1 hash of namespace UUID and name.
func NewV5(ns uuid.UUID, name string) *UUID {
	return &UUID{uuid.NewV5(ns, name)}
}

// GetUUID parses and checks for a valid UUID string, and returns Nil if not valid.
func GetUUID(input string) *UUID {
	return &UUID{uuid.FromStringOrNil(input)}
}
