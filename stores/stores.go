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
	"errors"
	"github.com/abcum/surreal/cnf"
)

var store Store

var stores = make(map[string]Storer)

// Setup initialises the selected backend for the database
func Setup(opts *cnf.Context) (e error) {

	_, exists := stores[opts.Db]

	if !exists {
		return errors.New("Store '" + opts.Db + "' is not registered")
	}

	store, e = stores[opts.Db](opts)

	return e

}

// Register registers a new backend with the database
func Register(name string, constructor Storer) (e error) {

	_, exists := stores[name]

	if exists {
		return errors.New("Store '" + name + "' is already registered")
	}

	stores[name] = constructor

	return nil

}

// Backend retrieves the currently selected backend
func Backend() Store {
	return store
}
