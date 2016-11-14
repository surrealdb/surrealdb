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
	kind int
	auth map[string]string
	conf map[string]string
}

func newOptions(c *fibre.Context) *options {
	return &options{
		kind: c.Get("kind").(int),
		auth: c.Get("auth").(map[string]string),
		conf: c.Get("conf").(map[string]string),
	}
}

func (o *options) get(kind int) (kv, ns, db string, err error) {

	kv = cnf.Settings.DB.Base
	ns = o.conf["NS"]
	db = o.conf["DB"]

	if o.kind > kind {
		err = &QueryError{}
		return
	}

	if ns == "" || db == "" {
		err = &BlankError{}
		return
	}

	return

}

func (o *options) ns(ns string) (err error) {

	// Check to see that the current user has
	// the necessary authentcation privileges
	// to be able to specify this namespace.

	if o.auth.Possible.NS != "*" && o.auth.Possible.NS != ns {
		return &NSError{NS: ns}
	}

	// Specify the NS on the context session, so
	// that it is remembered across requests on
	// any persistent connections.

	o.conf["NS"] = ns

	return

}

func (o *options) db(db string) (err error) {

	// Check to see that the current user has
	// the necessary authentcation privileges
	// to be able to specify this namespace.

	if o.auth.Possible.DB != "*" && o.auth.Possible.DB != db {
		return &DBError{DB: db}
	}

	// Specify the DB on the context session, so
	// that it is remembered across requests on
	// any persistent connections.

	o.conf["DB"] = db

	return

}
