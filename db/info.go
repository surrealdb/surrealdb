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

	db, err := e.dbo.AllDB(ast.NS)
	if err != nil {
		return nil, err
	}

	nt, err := e.dbo.AllNT(ast.NS)
	if err != nil {
		return nil, err
	}

	nu, err := e.dbo.AllNU(ast.NS)
	if err != nil {
		return nil, err
	}

	res := data.New()

	res.Object("database")
	for _, v := range db {
		res.Set(v.String(), "database", v.Name.ID)
	}

	res.Object("token")
	for _, v := range nt {
		res.Set(v.String(), "token", v.Name.ID)
	}

	res.Object("login")
	for _, v := range nu {
		res.Set(v.String(), "login", v.User.ID)
	}

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoDB(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	tb, err := e.dbo.AllTB(ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	dt, err := e.dbo.AllDT(ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	du, err := e.dbo.AllDU(ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	sc, err := e.dbo.AllSC(ast.NS, ast.DB)
	if err != nil {
		return nil, err
	}

	res := data.New()

	res.Object("table")
	for _, v := range tb {
		res.Set(v.String(), "table", v.Name.ID)
	}

	res.Object("token")
	for _, v := range dt {
		res.Set(v.String(), "token", v.Name.ID)
	}

	res.Object("login")
	for _, v := range du {
		res.Set(v.String(), "login", v.User.ID)
	}

	res.Object("scope")
	for _, v := range sc {
		res.Set(v.String(), "scope", v.Name.ID)
	}

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoTB(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	ev, err := e.dbo.AllEV(ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	fd, err := e.dbo.AllFD(ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	ix, err := e.dbo.AllIX(ast.NS, ast.DB, ast.What.TB)
	if err != nil {
		return nil, err
	}

	res := data.New()

	res.Object("event")
	for _, v := range ev {
		res.Set(v.String(), "event", v.Name.ID)
	}

	res.Object("field")
	for _, v := range fd {
		res.Set(v.String(), "field", v.Name.ID)
	}

	res.Object("index")
	for _, v := range ix {
		res.Set(v.String(), "index", v.Name.ID)
	}

	return []interface{}{res.Data()}, nil

}
