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
	"github.com/abcum/surreal/util/data"
)

// Event checks if any triggers are specified for this
// table, and executes them in name order.
func (d *document) event(ctx context.Context, met method) (err error) {

	// Check if this query has been run
	// in forced mode, because of an
	// index or foreign table update.

	forced := d.forced(ctx)

	// If this document has not changed
	// then there is no need to perform
	// any registered events.

	if !forced && !d.changed(ctx) {
		return nil
	}

	// Get the event values specified
	// for this table, loop through
	// them, and compute the events.

	evs, err := d.getEV()
	if err != nil {
		return err
	}

	if len(evs) > 0 {

		kind := ""

		switch met {
		case _CREATE:
			kind = "CREATE"
		case _UPDATE:
			kind = "UPDATE"
		case _DELETE:
			kind = "DELETE"
		}

		vars := data.New()
		vars.Set(d.id, varKeyThis)
		vars.Set(kind, varKeyMethod)
		vars.Set(d.current.Data(), varKeyAfter)
		vars.Set(d.initial.Data(), varKeyBefore)
		ctx = context.WithValue(ctx, ctxKeySpec, vars)

		ctx = context.WithValue(ctx, ctxKeyKind, cnf.AuthDB)

		for _, ev := range evs {

			val, err := d.i.e.fetch(ctx, ev.When, d.current)
			if err != nil {
				return err
			}

			switch v := val.(type) {
			case bool:
				switch v {
				case true:
					_, err = d.i.e.fetch(ctx, ev.Then, d.current)
					if err != nil {
						return err
					}
				}
			}

		}

	}

	return

}
