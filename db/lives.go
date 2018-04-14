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

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

// Lives checks if any table views are specified for
// this table, and executes them in name order.
func (d *document) lives(ctx context.Context, when method) (err error) {

	// Get the ID of the current fibre
	// connection so that we can check
	// against the ID of live queries.

	id := ctx.Value(ctxKeyId).(string)

	// If this document has not changed
	// then there is no need to update
	// any registered live queries.

	if !d.changed() {
		return nil
	}

	// Get the foreign read-only tables
	// specified for this table, and
	// update values which have changed.

	lvs, err := d.getLV()
	if err != nil {
		return err
	}

	if len(lvs) > 0 {

		for _, lv := range lvs {

			var ok bool
			var con *socket
			var doc *data.Doc

			if con, ok = sockets[lv.FB]; ok {

				ctx = con.ctx(d.ns, d.db)

				// Check whether the change was made by
				// the same connection as the live query,
				// and if it is then don't notify changes.

				if id == lv.FB {
					continue
				}

				// Check whether this live query has the
				// necessary permissions to view this
				// document, or continue to the next query.

				ok, err = d.grant(ctx, when)
				if err != nil {
					continue
				} else if !ok {
					continue
				}

				// Check whether this document matches the
				// filter conditions for the live query and
				// if not, then continue to the next query.

				ok, err = d.check(ctx, lv.Cond)
				if err != nil {
					continue
				} else if !ok {
					continue
				}

				switch lv.Diff {

				// If the live query has specified to only
				// receive diff changes, then there will be
				// no projected fields for this query.

				case true:

					doc = d.diff()

				// If the query has projected fields which it
				// wants to receive, then let's fetch these
				// fields, and return them to the socket.

				case false:

					for _, v := range lv.Expr {
						if _, ok := v.Expr.(*sql.All); ok {
							doc = d.current
							break
						}
					}

					if doc == nil {
						doc = data.New()
					}

					for _, e := range lv.Expr {
						switch v := e.Expr.(type) {
						case *sql.All:
							break
						default:
							v, err := d.i.e.fetch(ctx, v, d.current)
							if err != nil {
								continue
							}
							doc.Set(v, e.Field)
						}
					}

				}

				switch when {
				case _CREATE:
					con.queue(id, lv.ID, "CREATE", doc.Data())
				case _UPDATE:
					con.queue(id, lv.ID, "UPDATE", doc.Data())
				case _DELETE:
					con.queue(id, lv.ID, "DELETE", d.id)
				}

			}

		}

	}

	return

}
