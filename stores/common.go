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

package stores

import (
	"github.com/abcum/surreal/cnf"
)

// KeyValue is a specific object stored in the backend store
type KeyValue struct {
	Key   interface{}
	Value interface{}
}

type Store interface {
	Get(key interface{}) KeyValue
	Put(key, val interface{}) error
	Del(key interface{}) error
	Scan(beg, end interface{}, max int64) []KeyValue
}

type Storer func(*cnf.Context) (Store, error)
