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
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/keys"
)

func (e *executor) executeRemoveNamespaceStatement(txn kvs.TX, ast *sql.RemoveNamespaceStatement) (out []interface{}, err error) {

	// Remove the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.Name}
	_, err = txn.DelP(0, nkey.Encode(), 0)

	return

}

func (e *executor) executeRemoveDatabaseStatement(txn kvs.TX, ast *sql.RemoveDatabaseStatement) (out []interface{}, err error) {

	// Remove the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.Name}
	_, err = txn.DelP(0, dkey.Encode(), 0)

	return

}

func (e *executor) executeRemoveLoginStatement(txn kvs.TX, ast *sql.RemoveLoginStatement) (out []interface{}, err error) {

	if ast.Kind == sql.NAMESPACE {

		// Remove the login
		ukey := &keys.NU{KV: ast.KV, NS: ast.NS, US: ast.User}
		_, err = txn.DelP(0, ukey.Encode(), 0)

	}

	if ast.Kind == sql.DATABASE {

		// Remove the login
		ukey := &keys.DU{KV: ast.KV, NS: ast.NS, DB: ast.DB, US: ast.User}
		_, err = txn.DelP(0, ukey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveTokenStatement(txn kvs.TX, ast *sql.RemoveTokenStatement) (out []interface{}, err error) {

	if ast.Kind == sql.NAMESPACE {

		// Remove the token
		tkey := &keys.NT{KV: ast.KV, NS: ast.NS, TK: ast.Name}
		_, err = txn.DelP(0, tkey.Encode(), 0)

	}

	if ast.Kind == sql.DATABASE {

		// Remove the token
		tkey := &keys.DT{KV: ast.KV, NS: ast.NS, DB: ast.DB, TK: ast.Name}
		_, err = txn.DelP(0, tkey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveScopeStatement(txn kvs.TX, ast *sql.RemoveScopeStatement) (out []interface{}, err error) {

	// Remove the scope
	skey := &keys.SC{KV: ast.KV, NS: ast.NS, DB: ast.DB, SC: ast.Name}
	_, err = txn.DelP(0, skey.Encode(), 0)

	return

}

func (e *executor) executeRemoveTableStatement(txn kvs.TX, ast *sql.RemoveTableStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		// Remove the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		_, err = txn.DelP(0, tkey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveFieldStatement(txn kvs.TX, ast *sql.RemoveFieldStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		// Remove the field
		fkey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, FD: ast.Name}
		_, err = txn.DelP(0, fkey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveIndexStatement(txn kvs.TX, ast *sql.RemoveIndexStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		// Remove the index
		ikey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: ast.Name}
		_, err = txn.DelP(0, ikey.Encode(), 0)

		// Remove the index
		dkey := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: ast.Name, FD: keys.Ignore}
		_, err = txn.DelP(0, dkey.Encode(), 0)

	}

	return

}
