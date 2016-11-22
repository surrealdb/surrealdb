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

func (this *SC) GetTK(name string) *TK {
	if tk, ok := this.TK[name]; ok {
		return tk
	}
	return nil
}

func (this *SC) AddTK(ast *sql.DefineTokenStatement) {
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
