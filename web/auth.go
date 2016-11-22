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
	"strings"

	"encoding/base64"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"

	"github.com/dgrijalva/jwt-go"
)

func auth() fibre.MiddlewareFunc {
	return func(h fibre.HandlerFunc) fibre.HandlerFunc {
		return func(c *fibre.Context) (err error) {

			defer func() {
				if r := recover(); r != nil {
					err = fibre.NewHTTPError(403)
				}
			}()

			auth := &cnf.Auth{}
			c.Set("auth", auth)

			// Start off with an authentication level
			// which prevents running any sql queries,
			// and denies access to all data.

			auth.Kind = sql.AuthNO

			// Retrieve the current domain host and
			// if we are using a subdomain then set
			// the NS and DB to the subdomain bits.

			bits := strings.Split(c.Request().URL().Host, ".")
			subs := strings.Split(bits[0], "-")

			if len(subs) == 2 {
				auth.Kind = sql.AuthSC
				auth.Possible.NS = subs[0]
				auth.Selected.NS = subs[0]
				auth.Possible.DB = subs[1]
				auth.Selected.DB = subs[1]
			}

			// Retrieve the HTTP Authorization header
			// from the request, so that we can detect
			// whether it is Basic auth or Bearer auth.

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

						auth.Kind = sql.AuthKV
						auth.Possible.NS = "*"
						auth.Selected.NS = ""
						auth.Possible.DB = "*"
						auth.Selected.DB = ""

						return h(c)

					}

				}

			}

			// Check whether the Authorization header
			// is a Bearer Auth header, and if it is then
			// process this as default authentication.

			if head != "" && head[:6] == "Bearer" {

				var vars jwt.MapClaims
				var nok, dok, sok, tok, uok bool
				var nsv, dbv, scv, tkv, usv string

				token, err := jwt.Parse(head[7:], func(token *jwt.Token) (interface{}, error) {

					vars = token.Claims.(jwt.MapClaims)

					if err := vars.Valid(); err != nil {
						return nil, err
					}

					if val, ok := vars["auth"].(map[string]interface{}); ok {
						auth.Data = val
					}

					nsv, nok = vars["NS"].(string) // Namespace
					dbv, dok = vars["DB"].(string) // Database
					scv, sok = vars["SC"].(string) // Scope
					tkv, tok = vars["TK"].(string) // Token
					usv, uok = vars["US"].(string) // Login

					if tkv == "default" {
						if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
							return nil, fmt.Errorf("Unexpected signing method")
						}
					}

					if nok && dok && sok && tok {

						if tkv != "default" {
							key := mem.GetNS(nsv).GetDB(dbv).GetSC(scv).GetTK(tkv)
							if token.Header["alg"] != key.Type {
								return nil, fmt.Errorf("Unexpected signing method")
							}
							auth.Kind = sql.AuthSC
							return key.Code, nil
						} else {
							scp := mem.GetNS(nsv).GetDB(dbv).GetSC(scv)
							auth.Kind = sql.AuthSC
							return scp.Code, nil
						}

					} else if nok && dok && tok {

						if tkv != "default" {
							key := mem.GetNS(nsv).GetDB(dbv).GetTK(tkv)
							if token.Header["alg"] != key.Type {
								return nil, fmt.Errorf("Unexpected signing method")
							}
							auth.Kind = sql.AuthDB
							return key.Code, nil
						} else if uok {
							usr := mem.GetNS(nsv).GetDB(dbv).GetAC(usv)
							auth.Kind = sql.AuthDB
							return usr.Code, nil
						}

					} else if nok && tok {

						if tkv != "default" {
							key := mem.GetNS(nsv).GetTK(tkv)
							if token.Header["alg"] != key.Type {
								return nil, fmt.Errorf("Unexpected signing method")
							}
							auth.Kind = sql.AuthNS
							return key.Code, nil
						} else if uok {
							usr := mem.GetNS(nsv).GetAC(usv)
							auth.Kind = sql.AuthNS
							return usr.Code, nil
						}

					}

					return nil, fmt.Errorf("No available token")

				})

				if err == nil && token.Valid {

					if auth.Kind == sql.AuthNS {
						auth.Possible.NS = nsv
						auth.Selected.NS = nsv
						auth.Possible.DB = "*"
						auth.Selected.DB = ""
					}

					if auth.Kind == sql.AuthDB {
						auth.Possible.NS = nsv
						auth.Selected.NS = nsv
						auth.Possible.DB = dbv
						auth.Selected.DB = dbv
					}

					if auth.Kind == sql.AuthSC {
						auth.Possible.NS = nsv
						auth.Selected.NS = nsv
						auth.Possible.DB = dbv
						auth.Selected.DB = dbv
					}

					return h(c)

				}

			}

			return h(c)

		}
	}
}
