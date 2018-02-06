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

// New returns a new UUID (Version 4) using 16 random bytes or panics.
func New() *UUID {
	return &UUID{uuid.NewV4()}
}

// Parse parses and checks for a valid UUID string, and returns nil if not valid.
func Parse(input string) *UUID {
	id, err := uuid.FromString(input)
	if err != nil {
		return nil
	}
	return &UUID{id}
}
