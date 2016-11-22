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

import (
	"time"

	"github.com/abcum/surreal/sql"
)

var store map[string]*NS

type NS struct {
	AC   map[string]*AC
	TK   map[string]*TK
	DB   map[string]*DB
	Name string
}

type DB struct {
	AC   map[string]*AC
	TK   map[string]*TK
	SC   map[string]*SC
	TB   map[string]*TB
	Name string
}

type TB struct {
	FD   map[string]*FD
	IX   map[string]*IX
	Name string
}

type AC struct {
	User string
	Pass []byte
	Code []byte
}

type TK struct {
	Name string
	Type string
	Code []byte
}

type SC struct {
	TK     map[string]*TK
	Name   string
	Code   []byte
	Time   time.Duration
	Signup sql.Expr
	Signin sql.Expr
}

type FD struct {
	Name      string
	Type      string
	Enum      []interface{}
	Code      string
	Min       float64
	Max       float64
	Match     string
	Default   interface{}
	Notnull   bool
	Readonly  bool
	Mandatory bool
	Validate  bool
}

type IX struct {
	Name string
	Cols []string
	Uniq bool
}

func init() {
	store = make(map[string]*NS)
}
