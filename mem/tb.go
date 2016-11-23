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

func (this *TB) GetFD(name string) *FD {
	if fd, ok := this.FD[name]; ok {
		return fd
	}
	return nil
}

func (this *TB) AddFD(ast *sql.DefineFieldStatement) {
	if fd, ok := this.FD[ast.Name]; ok {
		fd.Name = ast.Name
		fd.Type = ast.Type
		fd.Enum = ast.Enum
		fd.Code = ast.Code
		fd.Min = ast.Min
		fd.Max = ast.Max
		fd.Match = ast.Match
		fd.Default = ast.Default
		fd.Notnull = ast.Notnull
		fd.Readonly = ast.Readonly
		fd.Mandatory = ast.Mandatory
		fd.Validate = ast.Validate
	} else {
		this.FD[ast.Name] = &FD{
			Name:      ast.Name,
			Type:      ast.Type,
			Enum:      ast.Enum,
			Code:      ast.Code,
			Min:       ast.Min,
			Max:       ast.Max,
			Match:     ast.Match,
			Default:   ast.Default,
			Notnull:   ast.Notnull,
			Readonly:  ast.Readonly,
			Mandatory: ast.Mandatory,
			Validate:  ast.Validate,
		}
	}
}

// --------------------------------------------------

func (this *TB) GetIX(name string) *IX {
	if ix, ok := this.IX[name]; ok {
		return ix
	}
	return nil
}

func (this *TB) AddIX(ast *sql.DefineIndexStatement) {
	if ix, ok := this.IX[ast.Name]; ok {
		ix.Name = ast.Name
		ix.Cols = ast.Cols
		ix.Uniq = ast.Uniq
	} else {
		this.IX[ast.Name] = &IX{
			Name: ast.Name,
			Cols: ast.Cols,
			Uniq: ast.Uniq,
		}
	}
}
