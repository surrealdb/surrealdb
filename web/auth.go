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
	"net"

	"bytes"
	"strings"

	"encoding/base64"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/mem"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/websocket"
)

func cidr(ip net.IP, networks []*net.IPNet) bool {
	for _, network := range networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func auth() fibre.MiddlewareFunc {
	return func(h fibre.HandlerFunc) fibre.HandlerFunc {
		return func(c *fibre.Context) (err error) {

			auth := &cnf.Auth{}
			c.Set("auth", auth)

			// Ensure that the authentication data
			// object is initiated at the beginning
			// so it is present when serialized.

			auth.Data = make(map[string]interface{})

			// Start off with an authentication level
			// which prevents running any sql queries,
			// and denies access to all data.

			auth.Kind = cnf.AuthNO

			// Set the default possible values for the
			// possible and selected namespace / database
			// so that they can be overridden.

			auth.Possible.NS = ""
			auth.Selected.NS = ""
			auth.Possible.DB = ""
			auth.Selected.DB = ""

			// Retrieve the current domain host and
			// if we are using a subdomain then set
			// the NS and DB to the subdomain bits.

			bits := strings.Split(c.Request().URL().Host, ".")
			subs := strings.Split(bits[0], "-")

			if len(subs) == 2 {
				auth.Kind = cnf.AuthSC
				auth.Possible.NS = subs[0]
				auth.Selected.NS = subs[0]
				auth.Possible.DB = subs[1]
				auth.Selected.DB = subs[1]
			}

			// If there is a namespace specified in
			// the request headers, then mark it as
			// the selected namespace.

			if ns := c.Request().Header().Get("NS"); len(ns) != 0 {
				auth.Kind = cnf.AuthSC
				auth.Possible.NS = ns
				auth.Selected.NS = ns
			}

			// If there is a database specified in
			// the request headers, then mark it as
			// the selected database.

			if db := c.Request().Header().Get("DB"); len(db) != 0 {
				auth.Kind = cnf.AuthSC
				auth.Possible.DB = db
				auth.Selected.DB = db
			}

			// Retrieve the HTTP Authorization header
			// from the request, so that we can detect
			// whether it is Basic auth or Bearer auth.

			head := c.Request().Header().Get("Authorization")

			// If there is no HTTP Authorization header,
			// check if there are websocket subprotocols
			// which might contain authn information.

			if len(head) == 0 {
				for _, prot := range websocket.Subprotocols(c.Request().Request) {
					if len(prot) > 7 && prot[0:7] == "bearer-" {
						head = "Bearer " + prot[7:]
						return checkBearer(c, prot[7:], func() error {
							return h(c)
						})
					}
				}
			}

			// Check whether the Authorization header
			// is a Basic Auth header, and if it is then
			// process this as root authentication.

			if len(head) > 0 && head[:5] == "Basic" {
				return checkMaster(c, head[6:], func() error {
					return h(c)
				})
			}

			// Check whether the Authorization header
			// is a Bearer Auth header, and if it is then
			// process this as default authentication.

			if len(head) > 0 && head[:6] == "Bearer" {
				return checkBearer(c, head[6:], func() error {
					return h(c)
				})
			}

			return h(c)

		}
	}
}

func checkRoot(c *fibre.Context, user, pass string, callback func() error) (err error) {

	auth := c.Get("auth").(*cnf.Auth)

	if cidr(c.IP(), cnf.Settings.Auth.Nets) {

		if user == cnf.Settings.Auth.User && pass == cnf.Settings.Auth.Pass {
			auth.Kind = cnf.AuthKV
			auth.Possible.NS = "*"
			auth.Possible.DB = "*"
		}

	}

	return callback()

}

func checkMaster(c *fibre.Context, info string, callback func() error) (err error) {

	auth := c.Get("auth").(*cnf.Auth)
	user := []byte(cnf.Settings.Auth.User)
	pass := []byte(cnf.Settings.Auth.Pass)

	base, err := base64.StdEncoding.DecodeString(info)

	if err == nil && cidr(c.IP(), cnf.Settings.Auth.Nets) {

		cred := bytes.SplitN(base, []byte(":"), 2)

		if len(cred) == 2 && bytes.Equal(cred[0], user) && bytes.Equal(cred[1], pass) {
			auth.Kind = cnf.AuthKV
			auth.Possible.NS = "*"
			auth.Possible.DB = "*"
		}

	}

	return callback()

}

func checkBearer(c *fibre.Context, info string, callback func() error) (err error) {

	auth := c.Get("auth").(*cnf.Auth)

	var txn kvs.TX
	var vars jwt.MapClaims
	var nok, dok, sok, tok, uok bool
	var nsv, dbv, scv, tkv, usv string

	// Start a new read transaction.

	if txn, err = db.Begin(false); err != nil {
		return fibre.NewHTTPError(500)
	}

	// Ensure the transaction closes.

	defer txn.Cancel()

	// Parse the specified JWT Token.

	token, err := jwt.Parse(info, func(token *jwt.Token) (interface{}, error) {

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

			scp, err := mem.New(txn).GetSC(nsv, dbv, scv)
			if err != nil {
				return nil, fmt.Errorf("Credentials failed")
			}

			auth.Data["scope"] = scp.Name

			if tkv != "default" {
				key, err := mem.New(txn).GetST(nsv, dbv, scv, tkv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				if token.Header["alg"] != key.Type {
					return nil, fmt.Errorf("Unexpected signing method")
				}
				auth.Kind = cnf.AuthSC
				return key.Code, nil
			} else {
				auth.Kind = cnf.AuthSC
				return scp.Code, nil
			}

		} else if nok && dok && tok {

			if tkv != "default" {
				key, err := mem.New(txn).GetDT(nsv, dbv, tkv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				if token.Header["alg"] != key.Type {
					return nil, fmt.Errorf("Unexpected signing method")
				}
				auth.Kind = cnf.AuthDB
				return key.Code, nil
			} else if uok {
				usr, err := mem.New(txn).GetDU(nsv, dbv, usv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				auth.Kind = cnf.AuthDB
				return usr.Code, nil
			}

		} else if nok && tok {

			if tkv != "default" {
				key, err := mem.New(txn).GetNT(nsv, tkv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				if token.Header["alg"] != key.Type {
					return nil, fmt.Errorf("Unexpected signing method")
				}
				auth.Kind = cnf.AuthNS
				return key.Code, nil
			} else if uok {
				usr, err := mem.New(txn).GetNU(nsv, usv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				auth.Kind = cnf.AuthNS
				return usr.Code, nil
			}

		}

		return nil, fmt.Errorf("No available token")

	})

	if err == nil && token.Valid {

		if auth.Kind == cnf.AuthNS {
			auth.Possible.NS = nsv
			auth.Selected.NS = nsv
			auth.Possible.DB = "*"
		}

		if auth.Kind == cnf.AuthDB {
			auth.Possible.NS = nsv
			auth.Selected.NS = nsv
			auth.Possible.DB = dbv
			auth.Selected.DB = dbv
		}

		if auth.Kind == cnf.AuthSC {
			auth.Possible.NS = nsv
			auth.Selected.NS = nsv
			auth.Possible.DB = dbv
			auth.Selected.DB = dbv
		}

	}

	return callback()

}
