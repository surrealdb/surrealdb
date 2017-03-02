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
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/keys"
)

func (e *executor) executeRemoveNamespaceStatement(ast *sql.RemoveNamespaceStatement) (out []interface{}, err error) {

	// Remove the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.Name.ID}
	_, err = e.txn.DelP(0, nkey.Encode(), 0)

	return

}

func (e *executor) executeRemoveDatabaseStatement(ast *sql.RemoveDatabaseStatement) (out []interface{}, err error) {

	// Remove the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.Name.ID}
	_, err = e.txn.DelP(0, dkey.Encode(), 0)

	return

}

func (e *executor) executeRemoveLoginStatement(ast *sql.RemoveLoginStatement) (out []interface{}, err error) {

	if ast.Kind == sql.NAMESPACE {

		// Remove the login
		ukey := &keys.NU{KV: ast.KV, NS: ast.NS, US: ast.User.ID}
		_, err = e.txn.DelP(0, ukey.Encode(), 0)

	}

	if ast.Kind == sql.DATABASE {

		// Remove the login
		ukey := &keys.DU{KV: ast.KV, NS: ast.NS, DB: ast.DB, US: ast.User.ID}
		_, err = e.txn.DelP(0, ukey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveTokenStatement(ast *sql.RemoveTokenStatement) (out []interface{}, err error) {

	if ast.Kind == sql.NAMESPACE {

		// Remove the token
		tkey := &keys.NT{KV: ast.KV, NS: ast.NS, TK: ast.Name.ID}
		_, err = e.txn.DelP(0, tkey.Encode(), 0)

	}

	if ast.Kind == sql.DATABASE {

		// Remove the token
		tkey := &keys.DT{KV: ast.KV, NS: ast.NS, DB: ast.DB, TK: ast.Name.ID}
		_, err = e.txn.DelP(0, tkey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveScopeStatement(ast *sql.RemoveScopeStatement) (out []interface{}, err error) {

	// Remove the scope
	skey := &keys.SC{KV: ast.KV, NS: ast.NS, DB: ast.DB, SC: ast.Name.ID}
	_, err = e.txn.DelP(0, skey.Encode(), 0)

	return

}

func (e *executor) executeRemoveTableStatement(ast *sql.RemoveTableStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		// Remove the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB}
		_, err = e.txn.DelP(0, tkey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveFieldStatement(ast *sql.RemoveFieldStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		// Remove the field
		fkey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, FD: ast.Name.ID}
		_, err = e.txn.DelP(0, fkey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveIndexStatement(ast *sql.RemoveIndexStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		// Remove the index
		ikey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, IX: ast.Name.ID}
		_, err = e.txn.DelP(0, ikey.Encode(), 0)

		// Remove the index
		dkey := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, IX: ast.Name.ID, FD: keys.Ignore}
		_, err = e.txn.DelP(0, dkey.Encode(), 0)

	}

	return

}
