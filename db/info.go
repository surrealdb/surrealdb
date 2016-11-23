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

package db

import (
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func (e *executor) executeInfoStatement(txn kvs.TX, ast *sql.InfoStatement) (out []interface{}, err error) {

	switch ast.Kind {
	case sql.NAMESPACE:
		return e.executeInfoNSStatement(txn, ast)
	case sql.DATABASE:
		return e.executeInfoDBStatement(txn, ast)
	case sql.TABLE:
		return e.executeInfoTBStatement(txn, ast)
	}

	return

}

func (e *executor) executeInfoNSStatement(txn kvs.TX, ast *sql.InfoStatement) (out []interface{}, err error) {

	res := data.New()
	res.Array("logins")
	res.Array("tokens")
	res.Array("databs")

	defer func() {
		if r := recover(); r != nil {
			out = append(out, res.Data())
		}
	}()

	db := mem.GetNS(ast.NS)

	for _, v := range db.AC {
		res.Inc(v, "logins")
	}

	for _, v := range db.TK {
		res.Inc(v, "tokens")
	}

	for _, v := range db.DB {
		res.Inc(v, "databs")
	}

	out = append(out, res.Data())

	return

}

func (e *executor) executeInfoDBStatement(txn kvs.TX, ast *sql.InfoStatement) (out []interface{}, err error) {

	res := data.New()
	res.Array("logins")
	res.Array("tokens")
	res.Array("scopes")
	res.Array("tables")

	defer func() {
		if r := recover(); r != nil {
			out = append(out, res.Data())
		}
	}()

	db := mem.GetNS(ast.NS).GetDB(ast.DB)

	for _, v := range db.AC {
		res.Inc(v, "logins")
	}

	for _, v := range db.TK {
		res.Inc(v, "tokens")
	}

	for _, v := range db.SC {
		res.Inc(v, "scopes")
	}

	for _, v := range db.TB {
		res.Inc(v, "tables")
	}

	out = append(out, res.Data())

	return

}

func (e *executor) executeInfoTBStatement(txn kvs.TX, ast *sql.InfoStatement) (out []interface{}, err error) {

	res := data.New()
	res.Array("fields")
	res.Array("indexs")

	defer func() {
		if r := recover(); r != nil {
			out = append(out, res.Data())
		}
	}()

	tb := mem.GetNS(ast.NS).GetDB(ast.DB).GetTB(ast.What)

	for _, v := range tb.RU {
		res.Inc(v, "rules")
	}

	for _, v := range tb.FD {
		res.Inc(v, "fields")
	}

	for _, v := range tb.IX {
		res.Inc(v, "indexs")
	}

	out = append(out, res.Data())

	return

}
