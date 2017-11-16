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

package sql

import (
	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
)

const (
	// Root access
	AuthKV int = iota
	// Namespace access
	AuthNS
	// Database access
	AuthDB
	// Scoped user access
	AuthSC
	// No access
	AuthNO
)

// options represents context runtime config.
type options struct {
	auth *cnf.Auth
}

func newOptions(c *fibre.Context) *options {
	return &options{
		auth: c.Get("auth").(*cnf.Auth),
	}
}

func (o *options) get(kind int) (kv, ns, db string, err error) {

	kv = cnf.Settings.DB.Base
	ns = o.auth.Selected.NS
	db = o.auth.Selected.DB

	if cnf.Kind(kind) < o.auth.Kind {
		err = &QueryError{}
		return
	}

	if cnf.Kind(kind) >= cnf.AuthNS && ns == "" {
		err = &BlankError{}
		return
	}

	if cnf.Kind(kind) >= cnf.AuthDB && db == "" {
		err = &BlankError{}
		return
	}

	return

}

func (o *options) ns(ns string) (err error) {

	// Check to see that the current user has
	// the necessary authentication privileges
	// to be able to specify this namespace.
	// This is only run if we are using the
	// KV, NS, or DB authentication levels, as
	// SC authentication levels make use of
	// table and field permissions instead.

	if o.auth.Kind < cnf.Kind(AuthSC) {
		if o.auth.Possible.NS != "*" && o.auth.Possible.NS != ns {
			return &PermsError{Resource: ns}
		}
	}

	// Specify the NS on the context session, so
	// that it is remembered across requests on
	// any persistent connections.

	o.auth.Selected.NS = ns

	return

}

func (o *options) db(db string) (err error) {

	// Check to see that the current user has
	// the necessary authentication privileges
	// to be able to specify this namespace.
	// This is only run if we are using the
	// KV, NS, or DB authentication levels, as
	// SC authentication levels make use of
	// table and field permissions instead.

	if o.auth.Kind < cnf.Kind(AuthSC) {
		if o.auth.Possible.DB != "*" && o.auth.Possible.DB != db {
			return &PermsError{Resource: db}
		}
	}

	// Specify the DB on the context session, so
	// that it is remembered across requests on
	// any persistent connections.

	o.auth.Selected.DB = db

	return

}
