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
	"fmt"
	"math"

	"encoding/json"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/diff"
	"github.com/abcum/surreal/util/keys"
)

var sep = `
-- ------------------------------
-- %s
-- ------------------------------
`

func export(c *fibre.Context, NS, DB string) error {

	w := c.Response()

	ctx := c.Context()

	exe := newExecutor()

	err := exe.begin(ctx, false)

	defer executorPool.Put(exe)

	// ------------------------------
	// Databases
	// ------------------------------

	dbs, err := exe.dbo.AllDB(ctx, NS)
	if err != nil {
		return err
	}

	if len(dbs) > 0 {
		fmt.Fprintf(w, sep, "DATABASE")
		fmt.Fprintln(w)
		for _, v := range dbs {
			fmt.Fprintf(w, "%s;\n", v)
		}
	}

	// ------------------------------
	// Tokens
	// ------------------------------

	dts, err := exe.dbo.AllDT(ctx, NS, DB)
	if err != nil {
		return err
	}

	if len(dts) > 0 {
		fmt.Fprintf(w, sep, "TOKENS")
		fmt.Fprintln(w)
		for _, v := range dts {
			fmt.Fprintf(w, "%s;\n", v)
		}
	}

	// ------------------------------
	// Logins
	// ------------------------------

	dus, err := exe.dbo.AllDU(ctx, NS, DB)
	if err != nil {
		return err
	}

	if len(dus) > 0 {
		fmt.Fprintf(w, sep, "LOGINS")
		fmt.Fprintln(w)
		for _, v := range dus {
			fmt.Fprintf(w, "%s;\n", v)
		}
	}

	// ------------------------------
	// Scopes
	// ------------------------------

	scs, err := exe.dbo.AllSC(ctx, NS, DB)
	if err != nil {
		return err
	}

	if len(scs) > 0 {

		fmt.Fprintf(w, sep, "SCOPES")

		fmt.Fprintln(w)

		for _, v := range scs {

			fmt.Fprintf(w, "%s;\n", v)

			// ------------------------------
			// Tokens
			// ------------------------------

			sct, err := exe.dbo.AllST(ctx, NS, DB, v.Name.VA)
			if err != nil {
				return err
			}

			if len(sct) > 0 {
				fmt.Fprintln(w)
				for _, v := range sct {
					fmt.Fprintf(w, "%s;\n", v)
				}
			}

			fmt.Fprintln(w)

		}

	}

	// ------------------------------
	// Tables
	// ------------------------------

	tbs, err := exe.dbo.AllTB(ctx, NS, DB)
	if err != nil {
		return err
	}

	for _, TB := range tbs {

		fmt.Fprintf(w, sep, "TABLE: "+TB.Name.VA)

		// ------------------------------
		// Remove
		// ------------------------------

		fmt.Fprintln(w)

		fmt.Fprintf(w, "%s;\n", &sql.RemoveTableStatement{
			What: sql.Tables{&sql.Table{TB.Name.VA}},
		})

		// ------------------------------
		// Define
		// ------------------------------

		fmt.Fprintln(w)

		fmt.Fprintf(w, "%s;\n", TB)

		// ------------------------------
		// Events
		// ------------------------------

		evs, err := exe.dbo.AllEV(ctx, NS, DB, TB.Name.VA)
		if err != nil {
			return err
		}

		if len(evs) > 0 {
			fmt.Fprintln(w)
			for _, v := range evs {
				fmt.Fprintf(w, "-- %s;\n", v)
			}
		}

		// ------------------------------
		// Fields
		// ------------------------------

		fds, err := exe.dbo.AllFD(ctx, NS, DB, TB.Name.VA)
		if err != nil {
			return err
		}

		if len(fds) > 0 {
			fmt.Fprintln(w)
			for _, v := range fds {
				fmt.Fprintf(w, "%s;\n", v)
			}
		}

		// ------------------------------
		// Indexes
		// ------------------------------

		ixs, err := exe.dbo.AllIX(ctx, NS, DB, TB.Name.VA)
		if err != nil {
			return err
		}

		if len(ixs) > 0 {
			fmt.Fprintln(w)
			for _, v := range ixs {
				fmt.Fprintf(w, "%s;\n", v)
			}
		}

	}

	// ------------------------------
	// BEGIN
	// ------------------------------

	fmt.Fprintf(w, sep, "TRANSACTION")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "BEGIN TRANSACTION;\n")

	// ------------------------------
	// DATA
	// ------------------------------

TB:
	for _, TB := range tbs {

		fmt.Fprintf(w, sep, "TABLE DATA: "+TB.Name.VA)
		fmt.Fprintln(w)

		beg := &keys.Thing{KV: cnf.Settings.DB.Base, NS: NS, DB: DB, TB: TB.Name.VA, ID: keys.Ignore}
		end := &keys.Thing{KV: cnf.Settings.DB.Base, NS: NS, DB: DB, TB: TB.Name.VA, ID: keys.Suffix}

		min, max := beg.Encode(), end.Encode()

		for x := 0; ; x = 1 {

			var err error
			var vls []kvs.KV

			if TB.Vers {
				vls, err = exe.dbo.AllR(ctx, min, max, 10000)
			} else {
				vls, err = exe.dbo.GetR(ctx, math.MaxInt64, min, max, 10000)
			}

			if err != nil {
				return err
			}

			// If there are no further records
			// fetched from the data layer, then
			// return out of this loop iteration.

			if x >= len(vls) {
				continue TB
			}

			// If there is at least 1 key-value
			// then loop over all the items and
			// process the records.

			o := data.New()
			n := data.New()
			p := new(keys.Thing)

			for _, kv := range vls {

				k := &keys.Thing{}
				k.Decode(kv.Key())

				v := kv.Ver()

				if kv.Exi() {

					n = data.New().Decode(kv.Val())

					if p.TB != k.TB || p.ID != k.ID {

						j, _ := n.MarshalJSON()

						if TB.Vers {
							fmt.Fprintf(w, "UPDATE ⟨%s⟩:⟨%s⟩ CONTENT %s VERSION %d;\n", k.TB, k.ID, j, v)
						} else {
							fmt.Fprintf(w, "UPDATE ⟨%s⟩:⟨%s⟩ CONTENT %s;\n", k.TB, k.ID, j)
						}

					} else {

						a, _ := o.Data().(map[string]interface{})
						b, _ := n.Data().(map[string]interface{})
						c := diff.Diff(a, b)
						j, _ := json.Marshal(c)

						if TB.Vers {
							fmt.Fprintf(w, "UPDATE ⟨%s⟩:⟨%s⟩ DIFF %s VERSION %d;\n", k.TB, k.ID, j, v)
						} else {
							fmt.Fprintf(w, "UPDATE ⟨%s⟩:⟨%s⟩ DIFF %s;\n", k.TB, k.ID, j)
						}

					}

				} else {

					if TB.Vers {
						fmt.Fprintf(w, "DELETE ⟨%s⟩:⟨%s⟩ VERSION %d;\n", k.TB, k.ID, v)
					} else {
						fmt.Fprintf(w, "DELETE ⟨%s⟩:⟨%s⟩;\n", k.TB, k.ID)
					}

				}

				p, o = k, n

			}

			// When we loop around, we will use
			// the key of the last retrieved key
			// to perform the next range request.

			beg.Decode(vls[len(vls)-1].Key())

			min = append(beg.Encode(), byte(0))

		}

	}

	// ------------------------------
	// COMMIT
	// ------------------------------

	fmt.Fprintf(w, sep, "TRANSACTION")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "COMMIT TRANSACTION;\n")

	return nil

}
