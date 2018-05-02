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

func (e *executor) executeInfo(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	switch ast.Kind {
	case sql.NAMESPACE:
		return e.executeInfoNS(ctx, ast)
	case sql.DATABASE:
		return e.executeInfoDB(ctx, ast)
	case sql.TABLE:
		return e.executeInfoTB(ctx, ast)
	}

	return

}

func (e *executor) executeInfoNS(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	db, err := e.dbo.AllDB(ctx, ast.NS)
	if err != nil {
		return nil, err
	}

	nt, err := e.dbo.AllNT(ctx, ast.NS)
	if err != nil {
		return nil, err
	}

	nu, err := e.dbo.AllNU(ctx, ast.NS)
	if err != nil {
		return nil, err
	}

	res := data.New()

	dbase := make(map[string]interface{})
	for _, v := range db {
		dbase[v.Name.ID] = v.String()
	}

	token := make(map[string]interface{})
	for _, v := range nt {
		token[v.Name.ID] = v.String()
	}

	login := make(map[string]interface{})
	for _, v := range nu {
		login[v.User.ID] = v.String()
	}

	res.Set(dbase, "database")
	res.Set(token, "token")
	res.Set(login, "login")

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoDB(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	tb, err := e.dbo.AllTB(ctx, ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	dt, err := e.dbo.AllDT(ctx, ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	du, err := e.dbo.AllDU(ctx, ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	sc, err := e.dbo.AllSC(ctx, ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	res := data.New()

	table := make(map[string]interface{})
	for _, v := range tb {
		table[v.Name.ID] = v.String()
	}

	token := make(map[string]interface{})
	for _, v := range dt {
		token[v.Name.ID] = v.String()
	}

	login := make(map[string]interface{})
	for _, v := range du {
		login[v.User.ID] = v.String()
	}

	scope := make(map[string]interface{})
	for _, v := range sc {
		scope[v.Name.ID] = v.String()
	}

	res.Set(table, "table")
	res.Set(token, "token")
	res.Set(login, "login")
	res.Set(scope, "scope")

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoTB(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	ev, err := e.dbo.AllEV(ctx, ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	fd, err := e.dbo.AllFD(ctx, ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	ix, err := e.dbo.AllIX(ctx, ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	lv, err := e.dbo.AllLV(ctx, ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	ft, err := e.dbo.AllFT(ctx, ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	res := data.New()

	event := make(map[string]interface{})
	for _, v := range ev {
		event[v.Name.ID] = v.String()
	}

	field := make(map[string]interface{})
	for _, v := range fd {
		field[v.Name.ID] = v.String()
	}

	index := make(map[string]interface{})
	for _, v := range ix {
		index[v.Name.ID] = v.String()
	}

	lives := make(map[string]interface{})
	for _, v := range lv {
		lives[v.ID] = v.String()
	}

	table := make(map[string]interface{})
	for _, v := range ft {
		table[v.Name.ID] = v.String()
	}

	res.Set(event, "event")
	res.Set(field, "field")
	res.Set(index, "index")
	res.Set(lives, "lives")
	res.Set(table, "table")

	return []interface{}{res.Data()}, nil

}
