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
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"
)

func signup(c *fibre.Context) (err error) {

	var vars map[string]interface{}

	c.Bind(&vars)

	n, nok := vars["NS"].(string)
	d, dok := vars["DB"].(string)
	s, sok := vars["SC"].(string)

	// If we have a namespace, database, and
	// scope defined, then we are logging in
	// to the scope level.

	if nok && len(n) > 0 && dok && len(d) > 0 && sok && len(s) > 0 {

		var txn kvs.TX
		var res []*db.Response
		var scp *sql.DefineScopeStatement

		// Start a new read transaction.

		if txn, err = db.Begin(false); err != nil {
			return fibre.NewHTTPError(500)
		}

		// Ensure the transaction closes.

		defer txn.Cancel()

		// Get the specified signin scope.

		if scp, err = mem.New(txn).GetSC(n, d, s); err != nil {
			log.WithFields(map[string]interface{}{
				"NS":  n,
				"DB":  d,
				"SC":  s,
				"url": "/signup",
			}).Debugln("Authentication scope does not exist")
			return fibre.NewHTTPError(403)
		}

		// Process the scope signup statement.

		qury := &sql.Query{Statements: []sql.Statement{scp.Signup}}

		if res, err = db.Process(c, qury, vars); err != nil {
			log.WithFields(map[string]interface{}{
				"NS":  n,
				"DB":  d,
				"SC":  s,
				"URL": "/signup",
			}).Debugln("Authentication scope signup was unsuccessful")
			return fibre.NewHTTPError(501)
		}

		if len(res) != 1 && len(res[0].Result) != 1 {
			log.WithFields(map[string]interface{}{
				"NS":  n,
				"DB":  d,
				"SC":  s,
				"URL": "/signup",
			}).Debugln("Authentication scope signup was unsuccessful")
			return fibre.NewHTTPError(403)
		}

		return c.Code(200)

	}

	return fibre.NewHTTPError(401)

}
