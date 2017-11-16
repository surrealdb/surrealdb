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
	"golang.org/x/crypto/bcrypt"
)

func signin(c *fibre.Context) (err error) {

	var vars map[string]interface{}

	c.Bind(&vars)

	n, nok := vars[varKeyNs].(string)
	d, dok := vars[varKeyDb].(string)
	s, sok := vars[varKeySc].(string)

	// Ensure that the IP address of the
	// user signing in is available so that
	// it can be used within signin queries.

	vars[varKeyIp] = c.IP().String()

	// Ensure that the website origin of the
	// user signing in is available so that
	// it can be used within signin queries.

	vars[varKeyOrigin] = c.Origin()

	// If we have a namespace, database, and
	// scope defined, then we are logging in
	// to the scope level.

	if nok && len(n) > 0 && dok && len(d) > 0 && sok && len(s) > 0 {

		var ok bool
		var txn kvs.TX
		var str string
		var doc *sql.Thing
		var res []*db.Response
		var exp *sql.SubExpression
		var scp *sql.DefineScopeStatement

		// Start a new read transaction.

		if txn, err = db.Begin(false); err != nil {
			return fibre.NewHTTPError(500)
		}

		// Ensure the transaction closes.

		defer txn.Cancel()

		// Get the specified signin scope.

		if scp, err = mem.NewWithTX(txn).GetSC(n, d, s); err != nil {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope does not exist")
		}

		// Check that the scope allows signin.

		if exp, ok = scp.Signin.(*sql.SubExpression); !ok {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signup was unsuccessful")
		}

		// Process the scope signin statement.

		c.Set(varKeyAuth, &cnf.Auth{Kind: cnf.AuthDB})

		query := &sql.Query{Statements: []sql.Statement{exp.Expr}}

		// If the query fails then return a 501 error.

		if res, err = db.Process(c, query, vars); err != nil {
			return fibre.NewHTTPError(501).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signin was unsuccessful")
		}

		// If the response is not 1 record then return a 403 error.

		if len(res) != 1 || len(res[0].Result) != 1 {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signin was unsuccessful")
		}

		// If the query does not return an id field then return a 403 error.

		if doc, ok = data.Consume(res[0].Result[0]).Get("id").Data().(*sql.Thing); !ok {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signin was unsuccessful")
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"NS":  n,
			"DB":  d,
			"SC":  s,
			"TK":  "default",
			"iss": "Surreal",
			"iat": time.Now().Unix(),
			"nbf": time.Now().Unix(),
			"exp": time.Now().Add(scp.Time).Unix(),
			"TB":  doc.TB,
			"ID":  doc.ID,
		})

		// Try to create the final signed token as a string.

		if str, err = signr.SignedString(scp.Code); err != nil {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Problem with signing string")
		}

		return c.Send(200, str)

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

		if !uok || !pok {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"du": u,
			}).WithMessage("Username or password is missing")
		}

		// Start a new read transaction.

		if usr, err = signinDB(n, d, u, p); err != nil {
			return err
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"US":  u,
			"NS":  n,
			"DB":  d,
			"TK":  "default",
			"iss": "Surreal",
			"iat": time.Now().Unix(),
			"nbf": time.Now().Unix(),
			"exp": time.Now().Add(1 * time.Hour).Unix(),
		})

		// Try to create the final signed token as a string.

		if str, err = signr.SignedString(usr.Code); err != nil {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"du": u,
			}).WithMessage("Problem with signing string")
		}

		return c.Send(200, str)

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

		if !uok || !pok {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"nu": u,
			}).WithMessage("Database signin was unsuccessful")
		}

		if usr, err = signinNS(n, u, p); err != nil {
			return err
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"US":  u,
			"NS":  n,
			"TK":  "default",
			"iss": "Surreal",
			"iat": time.Now().Unix(),
			"nbf": time.Now().Unix(),
			"exp": time.Now().Add(1 * time.Hour).Unix(),
		})

		// Try to create the final signed token as a string.

		if str, err = signr.SignedString(usr.Code); err != nil {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"nu": u,
			}).WithMessage("Problem with signing string")
		}

		return c.Send(200, str)

	}

	return fibre.NewHTTPError(403)

}

func signinDB(n, d, u, p string) (usr *sql.DefineLoginStatement, err error) {

	var txn kvs.TX

	// Start a new read transaction.

	if txn, err = db.Begin(false); err != nil {
		return nil, fibre.NewHTTPError(500)
	}

	// Ensure the transaction closes.

	defer txn.Cancel()

	// Get the specified user and password.

	if len(u) == 0 || len(p) == 0 {
		return nil, fibre.NewHTTPError(403).WithFields(map[string]interface{}{
			"ns": n,
			"nu": u,
		}).WithMessage("Database signin was unsuccessful")
	}

	// Get the specified namespace login.

	if usr, err = mem.NewWithTX(txn).GetDU(n, d, u); err != nil {
		return nil, fibre.NewHTTPError(403).WithFields(map[string]interface{}{
			"ns": n,
			"nu": u,
		}).WithMessage("Database login does not exist")
	}

	// Compare the hashed and stored passwords.

	if err = bcrypt.CompareHashAndPassword(usr.Pass, []byte(p)); err != nil {
		return nil, fibre.NewHTTPError(403).WithFields(map[string]interface{}{
			"ns": n,
			"nu": u,
		}).WithMessage("Database signin was unsuccessful")
	}

	return

}

func signinNS(n, u, p string) (usr *sql.DefineLoginStatement, err error) {

	var txn kvs.TX

	// Start a new read transaction.

	if txn, err = db.Begin(false); err != nil {
		return nil, fibre.NewHTTPError(500)
	}

	// Ensure the transaction closes.

	defer txn.Cancel()

	// Get the specified user and password.

	if len(u) == 0 || len(p) == 0 {
		return nil, fibre.NewHTTPError(403).WithFields(map[string]interface{}{
			"ns": n,
			"nu": u,
		}).WithMessage("Database signin was unsuccessful")
	}

	// Get the specified namespace login.

	if usr, err = mem.NewWithTX(txn).GetNU(n, u); err != nil {
		return nil, fibre.NewHTTPError(403).WithFields(map[string]interface{}{
			"ns": n,
			"nu": u,
		}).WithMessage("Namespace login does not exist")
	}

	// Compare the hashed and stored passwords.

	if err = bcrypt.CompareHashAndPassword(usr.Pass, []byte(p)); err != nil {
		return nil, fibre.NewHTTPError(403).WithFields(map[string]interface{}{
			"ns": n,
			"nu": u,
		}).WithMessage("Namespace signin was unsuccessful")
	}

	return

}
