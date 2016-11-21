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
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"

	"github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/bcrypt"
)

func signin(c *fibre.Context) (err error) {

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

		var str string
		var scp *mem.SC
		var res []*db.Response

		// Get the specified signin scope.

		if scp = mem.GetNS(n).GetDB(d).GetSC(s); scp == nil {
			return fibre.NewHTTPError(403)
		}

		// Process the scope signin statement.

		res, err = db.Process(c, &sql.Query{[]sql.Statement{scp.Signin}}, vars)
		if err != nil {
			return fibre.NewHTTPError(403)
		}

		if len(res) != 1 && len(res[0].Result) != 1 {
			return fibre.NewHTTPError(403)
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"NS":   n,
			"DB":   d,
			"SC":   s,
			"TK":   "default",
			"iss":  "Surreal",
			"iat":  time.Now().Unix(),
			"nbf":  time.Now().Unix(),
			"exp":  time.Now().Add(scp.Time).Unix(),
			"auth": res[0].Result[0],
		})

		// Try to create the final signed token as a string.

		str, err = signr.SignedString([]byte(scp.Uniq))
		if err != nil {
			return fibre.NewHTTPError(403)
		}

		return c.Text(200, str)

	}

	// If we have a namespace, database, but
	// no scope defined, then we are logging
	// in to the database level.

	if nok && len(n) > 0 && dok && len(d) > 0 {

		var str string
		var usr *mem.AC

		// Get the specified user and password.

		u, uok := vars["user"].(string)
		p, pok := vars["pass"].(string)

		if !uok || len(u) == 0 || !pok || len(p) == 0 {
			return fibre.NewHTTPError(403)
		}

		// Get the specified database login.

		if usr = mem.GetNS(n).GetDB(d).GetAC(u); usr == nil {
			return fibre.NewHTTPError(403)
		}

		// Compare the hashed and stored passwords.

		err = bcrypt.CompareHashAndPassword([]byte(usr.Pass), []byte(p))
		if err != nil {
			return fibre.NewHTTPError(403)
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
			"exp": time.Now().Add(10 * time.Minute).Unix(),
		})

		// Try to create the final signed token as a string.

		str, err = signr.SignedString([]byte(usr.Uniq))
		if err != nil {
			return fibre.NewHTTPError(403)
		}

		return c.Text(200, str)

	}

	// If we have a namespace, but no database,
	// or scope defined, then we are logging
	// in to the namespace level.

	if nok && len(n) > 0 {

		var str string
		var usr *mem.AC

		// Get the specified user and password.

		u, uok := vars["user"].(string)
		p, pok := vars["pass"].(string)

		if !uok || len(u) == 0 || !pok || len(p) == 0 {
			return fibre.NewHTTPError(403)
		}

		// Get the specified namespace login.

		if usr = mem.GetNS(n).GetAC(u); usr == nil {
			return fibre.NewHTTPError(403)
		}

		// Compare the hashed and stored passwords.

		err = bcrypt.CompareHashAndPassword([]byte(usr.Pass), []byte(p))
		if err != nil {
			return fibre.NewHTTPError(403)
		}

		// Create a new token signer with the default claims.

		signr := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"US":  u,
			"NS":  n,
			"TK":  "default",
			"iss": "Surreal",
			"iat": time.Now().Unix(),
			"nbf": time.Now().Unix(),
			"exp": time.Now().Add(10 * time.Minute).Unix(),
		})

		// Try to create the final signed token as a string.

		str, err = signr.SignedString([]byte(usr.Uniq))
		if err != nil {
			return fibre.NewHTTPError(403)
		}

		return c.Text(200, str)

	}

	return fibre.NewHTTPError(403)

}
