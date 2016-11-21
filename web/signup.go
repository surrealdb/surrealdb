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
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"
)

func signup(c *fibre.Context) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = fibre.NewHTTPError(403)
		}
	}()

	var vars map[string]interface{}

	c.Bind(&vars)

	n, nok := vars["NS"].(string)
	d, dok := vars["DB"].(string)
	s, sok := vars["SC"].(string)

	// If we have a namespace, database, and
	// scope defined, then we are logging in
	// to the scope level.

	if nok && len(n) > 0 && dok && len(d) > 0 && sok && len(s) > 0 {

		var scp *mem.SC
		var res []*db.Response

		// Get the specified signin scope.

		if scp = mem.GetNS(n).GetDB(d).GetSC(s); scp == nil {
			return fibre.NewHTTPError(403)
		}

		// Process the scope signup statement.

		res, err = db.Process(c, &sql.Query{[]sql.Statement{scp.Signup}}, vars)
		if err != nil {
			return fibre.NewHTTPError(403)
		}

		if len(res) != 1 && len(res[0].Result) != 1 {
			return fibre.NewHTTPError(403)
		}

		return c.Code(200)

	}

	return fibre.NewHTTPError(401)

}
