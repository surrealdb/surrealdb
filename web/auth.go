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
	"fmt"

	"bytes"
	"encoding/base64"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"

	"github.com/dgrijalva/jwt-go"
)

func auth() fibre.MiddlewareFunc {
	return func(h fibre.HandlerFunc) fibre.HandlerFunc {
		return func(c *fibre.Context) error {

			auth := map[string]string{"NS": "", "DB": ""}
			c.Set("auth", auth)

			conf := map[string]string{"NS": "", "DB": ""}
			c.Set("conf", conf)

			// Start off with an authentication level
			// which prevents running any sql queries,
			// and denies access to all data.

			c.Set("kind", sql.AuthNO)

			// Check whether there is an Authorization
			// header present, and if there is check
			// whether it is a Basic Auth header.

			// Retrieve the HTTP Authorization header
			// from the request, and continue.

			head := c.Request().Header().Get("Authorization")

			// Check whether the Authorization header
			// is a Basic Auth header, and if it is then
			// process this as root authentication.

			if head != "" && head[:5] == "Basic" {

				base, err := base64.StdEncoding.DecodeString(head[6:])

				if err == nil {

					user := []byte(cnf.Settings.Auth.User)
					pass := []byte(cnf.Settings.Auth.Pass)
					cred := bytes.SplitN(base, []byte(":"), 2)

					if len(cred) == 2 && bytes.Equal(cred[0], user) && bytes.Equal(cred[1], pass) {

						// ------------------------------
						// Root authentication
						// ------------------------------

						c.Set("kind", sql.AuthKV)
						auth["NS"] = "*" // Anything allowed
						conf["NS"] = ""  // Must specify
						auth["DB"] = "*" // Anything allowed
						conf["DB"] = ""  // Must specify

						return h(c)

					}

				}

			}

			// Check whether the Authorization header
			// is a Bearer Auth header, and if it is then
			// process this as default authentication.

			if head != "" && head[:6] == "Bearer" {

				token, err := jwt.Parse(head[7:], func(token *jwt.Token) (interface{}, error) {
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
					}
					return []byte(cnf.Settings.Auth.Token), nil
				})

				if err == nil && token.Valid {

					// ------------------------------
					// Namespace authentication
					// ------------------------------

					// c.Set("kind", sql.AuthNS)
					// auth["NS"] = "SPECIFIED" // Not allowed to change
					// conf["NS"] = "SPECIFIED" // Not allowed to change
					// auth["DB"] = "*"         // Anything allowed
					// conf["DB"] = ""          // Must specify

					// ------------------------------
					// Database authentication
					// ------------------------------

					// c.Set("kind", sql.AuthDB)
					// auth["NS"] = "SPECIFIED" // Not allowed to change
					// conf["NS"] = "SPECIFIED" // Not allowed to change
					// auth["DB"] = "SPECIFIED" // Not allowed to change
					// conf["DB"] = "SPECIFIED" // Not allowed to change

					// ------------------------------
					// Scoped authentication
					// ------------------------------

					// c.Set("kind", sql.AuthTB)
					// auth["NS"] = "SPECIFIED" // Not allowed to change
					// conf["NS"] = "SPECIFIED" // Not allowed to change
					// auth["DB"] = "SPECIFIED" // Not allowed to change
					// conf["DB"] = "SPECIFIED" // Not allowed to change

					return h(c)

				}

			}

			if c.Request().Header().Get("Upgrade") == "websocket" {
				return h(c)
			}

			return fibre.NewHTTPError(401)

		}
	}
}
