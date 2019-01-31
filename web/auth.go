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
	"github.com/abcum/surreal/sql"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/websocket"
)

var ignore = func() error {
	return nil
}

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

			// Initialise the connection authentication
			// information which will store whether the
			// connection has authenticated or not.

			auth := new(cnf.Auth)
			c.Set(varKeyAuth, auth)

			// Start off with an authentication level
			// which prevents running any sql queries,
			// and denies access to all data.

			auth.Kind = cnf.AuthNO

			// Retrieve the current domain host and
			// if we are using a subdomain then set
			// the NS and DB to the subdomain bits.

			bits := strings.Split(c.Request().URL().Host, ".")
			subs := strings.Split(bits[0], "-")

			if len(subs) == 2 {
				auth.NS = subs[0]
				auth.DB = subs[1]
			}

			// If there is a Session ID specified in
			// the request headers, then mark it as
			// the connection Session ID.

			if id := c.Request().Header().Get(varKeyId); len(id) != 0 {
				c.Set(varKeyUniq, id)
			}

			// If there is a namespace specified in
			// the request headers, then mark it as
			// the selected namespace.

			if ns := c.Request().Header().Get(varKeyNs); len(ns) != 0 {
				auth.NS = ns
			}

			// If there is a database specified in
			// the request headers, then mark it as
			// the selected database.

			if db := c.Request().Header().Get(varKeyDb); len(db) != 0 {
				auth.DB = db
			}

			// Retrieve the HTTP Authorization header
			// from the request, so that we can detect
			// whether it is Basic auth or Bearer auth.

			head := c.Request().Header().Get("Authorization")

			// Check whether the Authorization header
			// is a Basic Auth header, and if it is then
			// process this as root authentication.

			if len(head) > 6 && head[:5] == "Basic" {
				return checkBasics(c, head[6:], func() error {
					return h(c)
				})
			}

			// Check whether the Authorization header
			// is a Bearer Auth header, and if it is then
			// process this as default authentication.

			if len(head) > 7 && head[:6] == "Bearer" {
				return checkBearer(c, head[7:], func() error {
					return h(c)
				})
			}

			// If there is no HTTP Authorization header,
			// check to see if there are confuguration
			// options specified in the socket protocols.

			if len(head) == 0 {

				// If there is a Session ID specified as
				// one of the socket protocols then use
				// this as the connection Session ID.

				for _, prot := range websocket.Subprotocols(c.Request().Request) {
					if len(prot) > 3 && prot[0:3] == "id-" {
						c.Set(varKeyUniq, prot[3:])
					}
				}

				// If there is a NS configuration option
				// defined as one of the socket protocols
				// then use this as the selected NS.

				for _, prot := range websocket.Subprotocols(c.Request().Request) {
					if len(prot) > 3 && prot[0:3] == "ns-" {
						auth.NS = prot[3:]
					}
				}

				// If there is a DB configuration option
				// defined as one of the socket protocols
				// then use this as the selected DB.

				for _, prot := range websocket.Subprotocols(c.Request().Request) {
					if len(prot) > 3 && prot[0:3] == "db-" {
						auth.DB = prot[3:]
					}
				}

				// If there is a Auth configuration option
				// defined as one of the socket protocols
				// then use this as the auth information.

				for _, prot := range websocket.Subprotocols(c.Request().Request) {
					if len(prot) > 5 && prot[0:5] == "auth-" {
						if err := checkBasics(c, prot[5:], ignore); err == nil {
							return h(c)
						}
						if err := checkBearer(c, prot[5:], ignore); err == nil {
							return h(c)
						}
					}
				}

			}

			return h(c)

		}
	}
}

func checkBasics(c *fibre.Context, info string, callback func() error) (err error) {

	var base []byte
	var cred [][]byte

	auth := c.Get(varKeyAuth).(*cnf.Auth)
	user := []byte(cnf.Settings.Auth.User)
	pass := []byte(cnf.Settings.Auth.Pass)

	// Parse the base64 encoded basic auth data

	if base, err = base64.StdEncoding.DecodeString(info); err != nil {
		return fibre.NewHTTPError(401).WithMessage("Problem with basic auth data")
	}

	// Split the basic auth USER and PASS details

	if cred = bytes.SplitN(base, []byte(":"), 2); len(cred) != 2 {
		return fibre.NewHTTPError(401).WithMessage("Problem with basic auth data")
	}

	// Check to see if IP, USER, and PASS match server settings

	if bytes.Equal(cred[0], user) && bytes.Equal(cred[1], pass) {

		// FIXME does not work for IPv6 addresses

		if cidr(c.IP(), cnf.Settings.Auth.Nets) {
			auth.Kind = cnf.AuthKV
			return callback()
		}

		return fibre.NewHTTPError(403).WithMessage("IP invalid for root authentication")

	}

	// If no KV authentication, then try to authenticate as NS user

	if auth.NS != "" {

		n := auth.NS
		u := string(cred[0])
		p := string(cred[1])

		if _, err = signinNS(c, n, u, p); err == nil {
			auth.Kind = cnf.AuthNS
			auth.NS = n
			return callback()
		}

		// If no NS authentication, then try to authenticate as DB user

		if auth.DB != "" {

			n := auth.NS
			d := auth.DB
			u := string(cred[0])
			p := string(cred[1])

			if _, err = signinDB(c, n, d, u, p); err == nil {
				auth.Kind = cnf.AuthDB
				auth.NS = n
				auth.DB = d
				return callback()
			}

		}

	}

	return fibre.NewHTTPError(401).WithMessage("Invalid authentication details")

}

func checkBearer(c *fibre.Context, info string, callback func() error) (err error) {

	var txn kvs.TX
	var res []*db.Response
	var vars jwt.MapClaims
	var nsk, dbk, sck, tkk, usk, tbk, idk bool
	var nsv, dbv, scv, tkv, usv, tbv, idv string

	// Start a new read transaction.

	if txn, err = db.Begin(false); err != nil {
		return fibre.NewHTTPError(500)
	}

	// Ensure the transaction closes.

	defer txn.Cancel()

	// Get the current context.

	ctx := c.Context()

	// Setup the kvs layer cache.

	cache := mem.NewWithTX(txn)

	// Reset the auth data first.

	auth := c.Get(varKeyAuth).(*cnf.Auth).Reset()

	// Parse the specified JWT Token.

	token, err := jwt.Parse(info, func(token *jwt.Token) (interface{}, error) {

		vars = token.Claims.(jwt.MapClaims)

		if err := vars.Valid(); err != nil {
			return nil, err
		}

		nsv, nsk = vars[varKeyNs].(string) // Namespace
		dbv, dbk = vars[varKeyDb].(string) // Database
		scv, sck = vars[varKeySc].(string) // Scope
		tkv, tkk = vars[varKeyTk].(string) // Token
		usv, usk = vars[varKeyUs].(string) // Login
		tbv, tbk = vars[varKeyTb].(string) // Table
		idv, idk = vars[varKeyId].(string) // Thing

		if tkv == "default" {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("Unexpected signing method")
			}
		}

		if nsk && dbk && sck && tkk {

			scp, err := cache.GetSC(ctx, nsv, dbv, scv)
			if err != nil {
				return nil, fmt.Errorf("Credentials failed")
			}

			// Store the authenticated scope.

			auth.Scope = scp.Name.VA

			// Store the authenticated thing.

			if tbk && idk {
				auth.Data = sql.NewThing(tbv, idv)
			}

			// Check that the scope specifies connect.

			if exp, ok := scp.Connect.(*sql.SubExpression); ok {

				// Process the scope connect statement.

				c := fibre.NewContext(c.Request(), c.Response(), c.Fibre())

				c.Set(varKeyAuth, &cnf.Auth{Kind: cnf.AuthDB, NS: nsv, DB: dbv})

				qvars := map[string]interface{}{
					"id": auth.Data, "token": vars,
				}

				query := &sql.Query{Statements: []sql.Statement{exp.Expr}}

				// If the query fails then fail authentication.

				if res, err = db.Process(c, query, qvars); err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}

				// If the response is not 1 record then fail authentication.

				if len(res) != 1 || len(res[0].Result) != 1 {
					return nil, fmt.Errorf("Credentials failed")
				}

				auth.Data = res[0].Result[0]

			}

			if tkv != "default" {
				key, err := cache.GetST(ctx, nsv, dbv, scv, tkv)
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

		} else if nsk && dbk && tkk {

			if tkv != "default" {
				key, err := cache.GetDT(ctx, nsv, dbv, tkv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				if token.Header["alg"] != key.Type {
					return nil, fmt.Errorf("Unexpected signing method")
				}
				auth.Kind = cnf.AuthDB
				return key.Code, nil
			} else if usk {
				usr, err := cache.GetDU(ctx, nsv, dbv, usv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				auth.Kind = cnf.AuthDB
				return usr.Code, nil
			}

		} else if nsk && tkk {

			if tkv != "default" {
				key, err := cache.GetNT(ctx, nsv, tkv)
				if err != nil {
					return nil, fmt.Errorf("Credentials failed")
				}
				if token.Header["alg"] != key.Type {
					return nil, fmt.Errorf("Unexpected signing method")
				}
				auth.Kind = cnf.AuthNS
				return key.Code, nil
			} else if usk {
				usr, err := cache.GetNU(ctx, nsv, usv)
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
			auth.NS = nsv
		}

		if auth.Kind == cnf.AuthDB {
			auth.NS = nsv
			auth.DB = dbv
		}

		if auth.Kind == cnf.AuthSC {
			auth.NS = nsv
			auth.DB = dbv
		}

		return callback()

	} else {

		auth.Reset()

	}

	return fibre.NewHTTPError(401).WithMessage("Invalid authentication details: " + err.Error())

}
