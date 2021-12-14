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

package web

import (
	"time"

	"github.com/surrealdb/fibre"
	"github.com/surrealdb/surrealdb/cnf"
	"github.com/surrealdb/surrealdb/db"
	"github.com/surrealdb/surrealdb/sql"
	"github.com/surrealdb/surrealdb/txn"
	"github.com/surrealdb/surrealdb/util/data"
	"github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/bcrypt"
)

func signin(c *fibre.Context) (err error) {

	var vars map[string]interface{}

	c.Bind(&vars)

	str, err := signinInternal(c, vars)

	switch err {
	case nil:
		return c.Send(200, str)
	default:
		return err
	}

}

func signinRpc(c *fibre.Context, vars map[string]interface{}) (res interface{}, err error) {

	var str string

	str, err = signinInternal(c, vars)
	if err != nil {
		return nil, err
	}

	err = checkBearer(c, str, ignore)
	if err != nil {
		return nil, err
	}

	return str, nil

}

func signinInternal(c *fibre.Context, vars map[string]interface{}) (str string, err error) {

	n, nok := vars[varKeyNs].(string)
	d, dok := vars[varKeyDb].(string)
	s, sok := vars[varKeySc].(string)

	// If we have a namespace, database, and
	// scope defined, then we are logging in
	// to the scope level.

	if nok && len(n) > 0 && dok && len(d) > 0 && sok && len(s) > 0 {

		var ok bool
		var tx *txn.TX
		var doc *sql.Thing
		var res []*db.Response
		var exp *sql.SubExpression
		var evt *sql.MultExpression
		var scp *sql.DefineScopeStatement

		// Start a new read transaction.

		if tx, err = txn.New(c.Context(), false); err != nil {
			return str, fibre.NewHTTPError(500)
		}

		// Ensure the transaction closes.

		defer tx.Cancel()

		// Get the current context.

		ctx := c.Context()

		// Create a temporary context.

		t := fibre.NewContext(
			c.Request(),
			c.Response(),
			c.Fibre(),
		)

		// Ensure we copy the session id.

		t.Set(varKeyUniq, c.Get(varKeyUniq))

		// Give full permissions to scope.

		t.Set(varKeyAuth, &cnf.Auth{Kind: cnf.AuthDB, NS: n, DB: d})

		// Specify fields to show in logs.

		f := map[string]interface{}{"ns": n, "db": d, "sc": s}

		// Get the specified signin scope.

		if scp, err = tx.GetSC(ctx, n, d, s); err != nil {
			m := "Authentication scope does not exist"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// Check that the scope allows signin.

		if exp, ok = scp.Signin.(*sql.SubExpression); !ok {
			m := "Authentication scope does not allow signin"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// Process the scope signin statement.

		query := &sql.Query{Statements: []sql.Statement{exp.Expr}}

		// If the query fails then return a 501 error.

		if res, err = db.Process(t, query, vars); err != nil {
			m := "Authentication scope signin was unsuccessful: Query failed"
			return str, fibre.NewHTTPError(501).WithFields(f).WithMessage(m)
		}

		// If the response is not 1 record then return a 403 error.

		if len(res) != 1 {
			m := "Authentication scope signin was unsuccessful: Query failed"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// If the response has an error set then return a 403 error.

		if res[0].Status != "OK" {
			m := "Authentication scope signin was unsuccessful: " + res[0].Detail
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// If the response has no record set then return a 403 error.

		if len(res[0].Result) != 1 {
			m := "Authentication scope signin was unsuccessful: No record returned"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// If the query does not return an id field then return a 403 error.

		if doc, ok = data.Consume(res[0].Result[0]).Get("id").Data().(*sql.Thing); !ok {
			m := "Authentication scope signin was unsuccessful: No id field found"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"NS":  n,
			"DB":  d,
			"SC":  s,
			"TK":  "default",
			"IP":  c.IP().String(),
			"iss": "Surreal",
			"iat": time.Now().Unix(),
			"nbf": time.Now().Unix(),
			"exp": time.Now().Add(scp.Time).Unix(),
			"TB":  doc.TB,
			"ID":  doc.ID,
		})

		// Try to create the final signed token as a string.

		if str, err = signr.SignedString(scp.Code); err != nil {
			m := "Problem with signing method: " + err.Error()
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// Check that the scope allows signup.

		if evt, ok = scp.OnSignin.(*sql.MultExpression); ok {

			stmts := make([]sql.Statement, len(evt.Expr))

			for k := range evt.Expr {
				stmts[k] = evt.Expr[k]
			}

			query := &sql.Query{Statements: stmts}

			qvars := map[string]interface{}{
				"id": doc,
			}

			// If the query fails then return a 501 error.

			if res, err = db.Process(t, query, qvars); err != nil {
				m := "Authentication scope signin was unsuccessful: `ON SIGNIN` failed:" + err.Error()
				return str, fibre.NewHTTPError(501).WithFields(f).WithMessage(m)
			}

		}

		return str, err

	}

	// If we have a namespace, database, but
	// no scope defined, then we are logging
	// in to the database level.

	if nok && len(n) > 0 && dok && len(d) > 0 {

		var str string
		var usr *sql.DefineLoginStatement

		// Get the specified user and password.

		u, uok := vars[varKeyUser].(string)
		p, pok := vars[varKeyPass].(string)

		// Specify fields to show in logs.

		f := map[string]interface{}{"ns": n, "db": d, "du": u}

		// Check that the required fields exist.

		if !uok || !pok {
			m := "Username or password is missing"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// Start a new read transaction.

		if usr, err = signinDB(c, n, d, u, p); err != nil {
			return str, err
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"US":  u,
			"NS":  n,
			"DB":  d,
			"TK":  "default",
			"IP":  c.IP().String(),
			"iss": "Surreal",
			"iat": time.Now().Unix(),
			"nbf": time.Now().Unix(),
			"exp": time.Now().Add(1 * time.Hour).Unix(),
		})

		// Try to create the final signed token as a string.

		if str, err = signr.SignedString(usr.Code); err != nil {
			m := "Problem with signing method: " + err.Error()
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		return str, err

	}

	// If we have a namespace, but no database,
	// or scope defined, then we are logging
	// in to the namespace level.

	if nok && len(n) > 0 {

		var str string
		var usr *sql.DefineLoginStatement

		// Get the specified user and password.

		u, uok := vars[varKeyUser].(string)
		p, pok := vars[varKeyPass].(string)

		// Specify fields to show in logs.

		f := map[string]interface{}{"ns": n, "nu": u}

		// Check that the required fields exist.

		if !uok || !pok {
			m := "Username or password is missing"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		if usr, err = signinNS(c, n, u, p); err != nil {
			return str, err
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"US":  u,
			"NS":  n,
			"TK":  "default",
			"IP":  c.IP().String(),
			"iss": "Surreal",
			"iat": time.Now().Unix(),
			"nbf": time.Now().Unix(),
			"exp": time.Now().Add(1 * time.Hour).Unix(),
		})

		// Try to create the final signed token as a string.

		if str, err = signr.SignedString(usr.Code); err != nil {
			m := "Problem with signing method: " + err.Error()
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		return str, err

	}

	return str, fibre.NewHTTPError(403)

}

func signinDB(c *fibre.Context, n, d, u, p string) (usr *sql.DefineLoginStatement, err error) {

	var tx *txn.TX

	// Start a new read transaction.

	if tx, err = txn.New(c.Context(), false); err != nil {
		return nil, fibre.NewHTTPError(500)
	}

	// Ensure the transaction closes.

	defer tx.Cancel()

	// Get the current context.

	ctx := c.Context()

	// Specify fields to show in logs.

	f := map[string]interface{}{"ns": n, "db": d, "du": u}

	// Get the specified user and password.

	if len(u) == 0 || len(p) == 0 {
		m := "Database signin was unsuccessful"
		return nil, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
	}

	// Get the specified namespace login.

	if usr, err = tx.GetDU(ctx, n, d, u); err != nil {
		m := "Database login does not exist"
		return nil, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
	}

	// Compare the hashed and stored passwords.

	if err = bcrypt.CompareHashAndPassword(usr.Pass, []byte(p)); err != nil {
		m := "Database signin was unsuccessful"
		return nil, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
	}

	return

}

func signinNS(c *fibre.Context, n, u, p string) (usr *sql.DefineLoginStatement, err error) {

	var tx *txn.TX

	// Start a new read transaction.

	if tx, err = txn.New(c.Context(), false); err != nil {
		return nil, fibre.NewHTTPError(500)
	}

	// Ensure the transaction closes.

	defer tx.Cancel()

	// Get the current context.

	ctx := c.Context()

	// Specify fields to show in logs.

	f := map[string]interface{}{"ns": n, "nu": u}

	// Get the specified user and password.

	if len(u) == 0 || len(p) == 0 {
		m := "Namespace signin was unsuccessful"
		return nil, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
	}

	// Get the specified namespace login.

	if usr, err = tx.GetNU(ctx, n, u); err != nil {
		m := "Namespace login does not exist"
		return nil, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
	}

	// Compare the hashed and stored passwords.

	if err = bcrypt.CompareHashAndPassword(usr.Pass, []byte(p)); err != nil {
		m := "Namespace signin was unsuccessful"
		return nil, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
	}

	return

}
