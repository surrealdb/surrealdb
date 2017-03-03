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
	"context"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func (e *executor) executeInfoStatement(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	switch ast.Kind {
	case sql.NAMESPACE:
		return e.executeInfoNSStatement(ctx, ast)
	case sql.DATABASE:
		return e.executeInfoDBStatement(ctx, ast)
	case sql.TABLE:
		return e.executeInfoTBStatement(ctx, ast)
	}

	return

}

func (e *executor) executeInfoNSStatement(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	db, err := e.mem.AllDB(ast.NS)
	if err != nil {
		return nil, err
	}

	nt, err := e.mem.AllNT(ast.NS)
	if err != nil {
		return nil, err
	}

	nu, err := e.mem.AllNU(ast.NS)
	if err != nil {
		return nil, err
	}

	res := data.New()

	res.Array("databases")
	for _, v := range db {
		res.Inc(v.Name, "databases")
	}

	res.Array("tokens")
	for _, v := range nt {
		res.Inc(v.Name, "tokens")
	}

	res.Array("logins")
	for _, v := range nu {
		res.Inc(v.User, "logins")
	}

	out = append(out, res.Data())

	return

}

func (e *executor) executeInfoDBStatement(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	tb, err := e.mem.AllTB(ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	dt, err := e.mem.AllDT(ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	du, err := e.mem.AllDU(ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	res := data.New()

	res.Array("tables")
	for _, v := range tb {
		res.Inc(v, "tables")
	}

	res.Array("tokens")
	for _, v := range dt {
		res.Inc(v.Name, "tokens")
	}

	res.Array("logins")
	for _, v := range du {
		res.Inc(v.User, "logins")
	}

	out = append(out, res.Data())

	return

}

func (e *executor) executeInfoTBStatement(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	tb, err := e.mem.GetTB(ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	fd, err := e.mem.AllFD(ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	ix, err := e.mem.AllIX(ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	res := data.New()
	res.Set(tb.Full, "full")
	res.Set(tb.Perm, "perm")

	res.Array("indexes")
	for _, v := range ix {
		obj := map[string]interface{}{
			"name": v.Name,
			"cols": v.Cols,
			"uniq": v.Uniq,
		}
		res.Inc(obj, "indexes")
	}

	res.Array("fields")
	for _, v := range fd {
		obj := map[string]interface{}{
			"name":      v.Name,
			"type":      v.Type,
			"perm":      v.Perm,
			"enum":      v.Enum,
			"code":      v.Code,
			"min":       v.Min,
			"max":       v.Max,
			"match":     v.Match,
			"default":   v.Default,
			"notnull":   v.Notnull,
			"readonly":  v.Readonly,
			"mandatory": v.Mandatory,
			"validate":  v.Validate,
		}
		res.Inc(obj, "fields")
	}

	out = append(out, res.Data())

	return

}
