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
	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func signup(c *fibre.Context) (err error) {

	var vars map[string]interface{}

	c.Bind(&vars)

	n, nok := vars[varKeyNs].(string)
	d, dok := vars[varKeyDb].(string)
	s, sok := vars[varKeySc].(string)

	// Ensure that the IP address of the
	// user signing up is available so that
	// it can be used within signup queries.

	vars[varKeyIp] = c.IP().String()

	// Ensure that the website origin of the
	// user signing up is available so that
	// it can be used within signup queries.

	vars[varKeyOrigin] = c.Origin()

	// If we have a namespace, database, and
	// scope defined, then we are logging in
	// to the scope level.

	if nok && len(n) > 0 && dok && len(d) > 0 && sok && len(s) > 0 {

		var ok bool
		var txn kvs.TX
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

		// Check that the scope allows signup.

		if exp, ok = scp.Signup.(*sql.SubExpression); !ok {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signup was unsuccessful")
		}

		// Process the scope signup statement.

		c.Set(varKeyAuth, &cnf.Auth{Kind: cnf.AuthDB})

		query := &sql.Query{Statements: []sql.Statement{exp.Expr}}

		// If the query fails then return a 501 error.

		if res, err = db.Process(c, query, vars); err != nil {
			return fibre.NewHTTPError(501).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signup was unsuccessful")
		}

		// If the response is not 1 record then return a 403 error.

		if len(res) != 1 || len(res[0].Result) != 1 {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signup was unsuccessful")
		}

		// If the query does not return an id field then return a 403 error.

		if _, ok = data.Consume(res[0].Result[0]).Get("id").Data().(*sql.Thing); !ok {
			return fibre.NewHTTPError(403).WithFields(map[string]interface{}{
				"ns": n,
				"db": d,
				"sc": s,
			}).WithMessage("Authentication scope signup was unsuccessful")
		}

		return c.Code(204)

	}

	return fibre.NewHTTPError(403)

}
