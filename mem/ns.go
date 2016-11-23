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

func (this *NS) GetAC(name string) *AC {
	this.RLock()
	defer this.RUnlock()
	if ac, ok := this.AC[name]; ok {
		return ac
	}
	return nil
}

func (this *NS) AddAC(ast *sql.DefineLoginStatement) {
	this.RLock()
	defer this.RUnlock()
	if ac, ok := this.AC[ast.User]; ok {
		ac.User = ast.User
		ac.Pass = ast.Pass
	} else {
		this.AC[ast.User] = &AC{
			User: ast.User,
			Pass: ast.Pass,
		}
	}
}

// --------------------------------------------------

func (this *NS) GetTK(name string) *TK {
	this.RLock()
	defer this.RUnlock()
	if tk, ok := this.TK[name]; ok {
		return tk
	}
	return nil
}

func (this *NS) AddTK(ast *sql.DefineTokenStatement) {
	this.RLock()
	defer this.RUnlock()
	if tk, ok := this.TK[ast.Name]; ok {
		tk.Name = ast.Name
		tk.Code = ast.Code
	} else {
		this.TK[ast.Name] = &TK{
			Name: ast.Name,
			Code: ast.Code,
		}
	}
}

// --------------------------------------------------

func (this *NS) GetDB(name string) *DB {
	this.RLock()
	defer this.RUnlock()
	if db, ok := this.DB[name]; ok {
		return db
	}
	return nil
}

func (this *NS) AddDB(ast *sql.DefineDatabaseStatement) {
	this.RLock()
	defer this.RUnlock()
	if db, ok := this.DB[ast.Name]; ok {
		db.Name = ast.Name
	} else {
		this.DB[ast.Name] = &DB{
			Name: ast.Name,
			AC:   make(map[string]*AC),
			TK:   make(map[string]*TK),
			SC:   make(map[string]*SC),
			TB:   make(map[string]*TB),
		}
	}
}
