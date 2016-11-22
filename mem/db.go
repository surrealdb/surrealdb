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

func (this *DB) GetAC(name string) *AC {
	if ac, ok := this.AC[name]; ok {
		return ac
	}
	return nil
}

func (this *DB) AddAC(ast *sql.DefineLoginStatement) {
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

func (this *DB) GetTK(name string) *TK {
	if tk, ok := this.TK[name]; ok {
		return tk
	}
	return nil
}

func (this *DB) AddTK(ast *sql.DefineTokenStatement) {
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

func (this *DB) GetSC(name string) *SC {
	if sc, ok := this.SC[name]; ok {
		return sc
	}
	return nil
}

func (this *DB) AddSC(ast *sql.DefineScopeStatement) {
	if sc, ok := this.SC[ast.Name]; ok {
		sc.Name = ast.Name
		sc.Time = ast.Time
		sc.Signup = ast.Signup
		sc.Signin = ast.Signin
	} else {
		this.SC[ast.Name] = &SC{
			Name:   ast.Name,
			Time:   ast.Time,
			Signup: ast.Signup,
			Signin: ast.Signin,
		}
	}
}

// --------------------------------------------------

func (this *DB) GetTB(name string) *TB {
	if tb, ok := this.TB[name]; ok {
		return tb
	}
	return nil
}

func (this *DB) AddTB(ast *sql.DefineTableStatement) {
	for _, name := range ast.What {
		if tb, ok := this.TB[name]; ok {
			tb.Name = name
		} else {
			this.TB[name] = &TB{
				Name: name,
				FD:   make(map[string]*FD),
			}
		}
	}
}
