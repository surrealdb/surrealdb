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

package mem

import "github.com/abcum/surreal/sql"

// --------------------------------------------------

func GetNS(name string) *NS {
	store.RLock()
	defer store.RUnlock()
	if ns, ok := store.NS[name]; ok {
		return ns
	}
	return nil
}

func AddNS(ast *sql.DefineNamespaceStatement) {
	store.RLock()
	defer store.RUnlock()
	if ns, ok := store.NS[ast.Name]; ok {
		ns.Name = ast.Name
	} else {
		store.NS[ast.Name] = &NS{
			Name: ast.Name,
			AC:   make(map[string]*AC),
			TK:   make(map[string]*TK),
			DB:   make(map[string]*DB),
		}
	}
}
