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

package web

import (
	"time"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/dgrijalva/jwt-go"
)

func signup(c *fibre.Context) (err error) {

	var vars map[string]interface{}

	c.Bind(&vars)

	str, err := signupInternal(c, vars)

	switch err {
	case nil:
		return c.Send(200, str)
	default:
		return err
	}

}

func signupRpc(c *fibre.Context, vars map[string]interface{}) (res interface{}, err error) {

	var str string

	str, err = signupInternal(c, vars)
	if err != nil {
		return nil, err
	}

	err = checkBearer(c, str, ignore)
	if err != nil {
		return nil, err
	}

	return str, nil

}

func signupInternal(c *fibre.Context, vars map[string]interface{}) (str string, err error) {

	n, nok := vars[varKeyNs].(string)
	d, dok := vars[varKeyDb].(string)
	s, sok := vars[varKeySc].(string)

	// If we have a namespace, database, and
	// scope defined, then we are logging in
	// to the scope level.

	if nok && len(n) > 0 && dok && len(d) > 0 && sok && len(s) > 0 {

		var ok bool
		var txn kvs.TX
		var doc *sql.Thing
		var res []*db.Response
		var exp *sql.SubExpression
		var evt *sql.MultExpression
		var scp *sql.DefineScopeStatement

		// Start a new read transaction.

		if txn, err = db.Begin(false); err != nil {
			return str, fibre.NewHTTPError(500)
		}

		// Ensure the transaction closes.

		defer txn.Cancel()

		// Get the current context.

		ctx := c.Context()

		// Create a temporary context.

		t := fibre.NewContext(
			c.Request(),
			c.Response(),
			c.Fibre(),
		)

		// Ensure we copy the session od.

		t.Set(varKeyUniq, c.Get(varKeyUniq))

		// Give full permissions to scope.

		t.Set(varKeyAuth, &cnf.Auth{Kind: cnf.AuthDB})

		// Specify fields to show in logs.

		f := map[string]interface{}{"ns": n, "db": d, "sc": s}

		// Get the specified signin scope.

		if scp, err = mem.NewWithTX(txn).GetSC(ctx, n, d, s); err != nil {
			m := "Authentication scope does not exist"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// Check that the scope allows signup.

		if exp, ok = scp.Signup.(*sql.SubExpression); !ok {
			m := "Authentication scope does not allow signup"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// Process the scope signup statement.

		query := &sql.Query{Statements: []sql.Statement{exp.Expr}}

		// If the query fails then return a 501 error.

		if res, err = db.Process(t, query, vars); err != nil {
			m := "Authentication scope signup was unsuccessful: Query failed"
			return str, fibre.NewHTTPError(501).WithFields(f).WithMessage(m)
		}

		// If the response is not 1 record then return a 403 error.

		if len(res) != 1 {
			m := "Authentication scope signup was unsuccessful: Query failed"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// If the response has an error set then return a 403 error.

		if res[0].Status != "OK" {
			m := "Authentication scope signin was unsuccessful: " + res[0].Detail
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// If the response has no record set then return a 403 error.

		if len(res[0].Result) != 1 {
			m := "Authentication scope signup was unsuccessful: No record created"
			return str, fibre.NewHTTPError(403).WithFields(f).WithMessage(m)
		}

		// If the query does not return an id field then return a 403 error.

		if doc, ok = data.Consume(res[0].Result[0]).Get("id").Data().(*sql.Thing); !ok {
			m := "Authentication scope signup was unsuccessful: No id field found"
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

		if evt, ok = scp.OnSignup.(*sql.MultExpression); ok {

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
				m := "Authentication scope signup was unsuccessful: `ON SIGNUP` failed:" + err.Error()
				return str, fibre.NewHTTPError(501).WithFields(f).WithMessage(m)
			}

		}

		return str, err

	}

	return str, fibre.NewHTTPError(403)

}
