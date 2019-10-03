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

package db

import (
	"context"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func (d *document) perms(ctx context.Context, doc *data.Doc) (err error) {

	// If this is a document loaded from
	// a subquery or data param, and not
	// from the KV store, then there is
	// no need to check permissions.

	if d.key == nil {
		return nil
	}

	// If we are authenticated using DB, NS,
	// or KV permissions level, then we can
	// ignore all permissions checks.

	if perm(ctx) < cnf.AuthSC {
		return nil
	}

	// Get the field definitions so we can
	// check if the permissions allow us
	// to view each field.

	fds, err := d.i.e.dbo.AllFD(ctx, d.key.NS, d.key.DB, d.key.TB)
	if err != nil {
		return err
	}

	// Once we have the table we reset the
	// context to DB level so that no other
	// embedded permissions are checked on
	// records within these permissions.

	ctx = context.WithValue(ctx, ctxKeyKind, cnf.AuthDB)

	// We then try to process the relevant
	// permissions dependent on the query
	// that we are currently processing. If
	// there are no permissions specified
	// for this table, then because this is
	// a scoped request, return an error.

	for _, fd := range fds {

		if fd.Perms != nil {

			err = doc.Walk(func(key string, val interface{}, exi bool) error {

				// We are checking the permissions of the field

				if p, ok := fd.Perms.(*sql.PermExpression); ok {

					// Get the old value

					old := d.initial.Get(key).Data()

					// Reset the variables

					vars := data.New()
					vars.Set(val, varKeyValue)
					vars.Set(val, varKeyAfter)
					vars.Set(old, varKeyBefore)
					ctx = context.WithValue(ctx, ctxKeySpec, vars)

					if v, err := d.i.e.fetch(ctx, p.Select, doc); err != nil {
						return err
					} else if b, ok := v.(bool); !ok || !b {
						doc.Del(key)
					}

				}

				return nil

			}, fd.Name.VA)

		}

	}

	return nil

}
