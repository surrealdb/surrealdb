// Copyright © 2016 SurrealDB Ltd.
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

	"github.com/surrealdb/surrealdb/cnf"
	"github.com/surrealdb/surrealdb/sql"
	"github.com/surrealdb/surrealdb/util/data"
)

func (e *executor) executeInfo(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	switch ast.Kind {
	case sql.ALL:
		return e.executeInfoKV(ctx, ast)
	case sql.NAMESPACE, sql.NS:
		return e.executeInfoNS(ctx, ast)
	case sql.DATABASE, sql.DB:
		return e.executeInfoDB(ctx, ast)
	case sql.SCOPE:
		return e.executeInfoSC(ctx, ast)
	case sql.TABLE:
		return e.executeInfoTB(ctx, ast)
	}

	return

}

func (e *executor) executeInfoKV(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthKV); err != nil {
		return nil, err
	}

	ns, err := e.tx.AllNS(ctx)
	if err != nil {
		return nil, err
	}

	res := data.New()

	nspac := make(map[string]interface{})
	for _, v := range ns {
		nspac[v.Name.VA] = v.String()
	}

	res.Set(nspac, "namespace")

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoNS(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthNS); err != nil {
		return nil, err
	}

	db, err := e.tx.AllDB(ctx, e.ns)
	if err != nil {
		return nil, err
	}

	nt, err := e.tx.AllNT(ctx, e.ns)
	if err != nil {
		return nil, err
	}

	nu, err := e.tx.AllNU(ctx, e.ns)
	if err != nil {
		return nil, err
	}

	res := data.New()

	dbase := make(map[string]interface{})
	for _, v := range db {
		dbase[v.Name.VA] = v.String()
	}

	token := make(map[string]interface{})
	for _, v := range nt {
		token[v.Name.VA] = v.String()
	}

	login := make(map[string]interface{})
	for _, v := range nu {
		login[v.User.VA] = v.String()
	}

	res.Set(dbase, "database")
	res.Set(token, "token")
	res.Set(login, "login")

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoDB(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	tb, err := e.tx.AllTB(ctx, e.ns, e.db)
	if err != nil {
		return nil, err
	}

	dt, err := e.tx.AllDT(ctx, e.ns, e.db)
	if err != nil {
		return nil, err
	}

	du, err := e.tx.AllDU(ctx, e.ns, e.db)
	if err != nil {
		return nil, err
	}

	sc, err := e.tx.AllSC(ctx, e.ns, e.db)
	if err != nil {
		return nil, err
	}

	res := data.New()

	table := make(map[string]interface{})
	for _, v := range tb {
		table[v.Name.VA] = v.String()
	}

	token := make(map[string]interface{})
	for _, v := range dt {
		token[v.Name.VA] = v.String()
	}

	login := make(map[string]interface{})
	for _, v := range du {
		login[v.User.VA] = v.String()
	}

	scope := make(map[string]interface{})
	for _, v := range sc {
		scope[v.Name.VA] = v.String()
	}

	res.Set(table, "table")
	res.Set(token, "token")
	res.Set(login, "login")
	res.Set(scope, "scope")

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoSC(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	st, err := e.tx.AllST(ctx, e.ns, e.db, ast.What.VA)
	if err != nil {
		return nil, err
	}

	res := data.New()

	token := make(map[string]interface{})
	for _, v := range st {
		token[v.Name.VA] = v.String()
	}

	res.Set(token, "token")

	return []interface{}{res.Data()}, nil

}

func (e *executor) executeInfoTB(ctx context.Context, ast *sql.InfoStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	ev, err := e.tx.AllEV(ctx, e.ns, e.db, ast.What.VA)
	if err != nil {
		return nil, err
	}

	fd, err := e.tx.AllFD(ctx, e.ns, e.db, ast.What.VA)
	if err != nil {
		return nil, err
	}

	ix, err := e.tx.AllIX(ctx, e.ns, e.db, ast.What.VA)
	if err != nil {
		return nil, err
	}

	ft, err := e.tx.AllFT(ctx, e.ns, e.db, ast.What.VA)
	if err != nil {
		return nil, err
	}

	lv, err := e.tx.AllLV(ctx, e.ns, e.db, ast.What.VA)
	if err != nil {
		return nil, err
	}

	res := data.New()

	event := make(map[string]interface{})
	for _, v := range ev {
		event[v.Name.VA] = v.String()
	}

	field := make(map[string]interface{})
	for _, v := range fd {
		field[v.Name.VA] = v.String()
	}

	index := make(map[string]interface{})
	for _, v := range ix {
		index[v.Name.VA] = v.String()
	}

	table := make(map[string]interface{})
	for _, v := range ft {
		table[v.Name.VA] = v.String()
	}

	lives := make(map[string]interface{})
	for _, v := range lv {
		lives[v.ID] = v.String()
	}

	res.Set(event, "event")
	res.Set(field, "field")
	res.Set(index, "index")
	res.Set(table, "table")
	res.Set(lives, "lives")

	return []interface{}{res.Data()}, nil

}
