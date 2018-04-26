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
)

// Lives checks if any table views are specified for
// this table, and executes them in name order.
func (d *document) lives(ctx context.Context, when method) (err error) {

	// If this document has not changed
	// then there is no need to update
	// any registered live queries.

	if !d.changed() {
		return nil
	}

	// Get the ID of the current fibre
	// connection so that we can check
	// against the ID of live queries.

	id := ctx.Value(ctxKeyId).(string)

	// Get the foreign read-only tables
	// specified for this table, and
	// update values which have changed.

	lvs, err := d.getLV()
	if err != nil {
		return err
	}

	// Loop over the currently running
	// live queries so that we can pass
	// change notifications to the socket.

	for _, lv := range lvs {

		// Check whether the change was made by
		// the same connection as the live query,
		// and if it is then don't notify changes.

		if id == lv.FB {
			continue
		}

		// Load the socket which owns the live
		// query so that we can check the socket
		// permissions, and send the notifications.

		if sck, ok := sockets.Load(lv.FB); ok {

			var out interface{}

			// Create a new context for this socket
			// which has the correct connection
			// variables, and auth levels.

			ctx = sck.(*socket).ctx(d.ns, d.db)

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

				out, _ = d.yield(ctx, lv, sql.DIFF)

			// If the query has projected fields which it
			// wants to receive, then let's fetch these
			// fields, and return them to the socket.

			case false:

				out, _ = d.yield(ctx, lv, sql.ILLEGAL)

			}

			switch when {
			case _DELETE:
				sck.(*socket).queue(id, lv.ID, "DELETE", d.id)
			case _CREATE:
				if out != nil {
					sck.(*socket).queue(id, lv.ID, "CREATE", out)
				}
			case _UPDATE:
				if out != nil {
					sck.(*socket).queue(id, lv.ID, "UPDATE", out)
				}
			}

		}

	}

	return

}
