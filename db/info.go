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
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/pack"
)

func executeInfoStatement(txn kvs.TX, ast *sql.InfoStatement) (out []interface{}, err error) {

	if ast.What == "" {

		res := data.New()
		res.Array("scopes")
		res.Array("tables")
		res.Array("views")

		// Get the scope definitions
		sbeg := &keys.SC{KV: ast.KV, NS: ast.NS, DB: ast.DB, SC: keys.Prefix}
		send := &keys.SC{KV: ast.KV, NS: ast.NS, DB: ast.DB, SC: keys.Suffix}
		skvs, _ := txn.RGet(sbeg.Encode(), send.Encode(), 0)
		for _, kv := range skvs {
			key := &keys.SC{}
			key.Decode(kv.Key())
			res.Inc(key.SC, "scopes")
		}

		// Get the table definitions
		tbeg := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: keys.Prefix}
		tend := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: keys.Suffix}
		tkvs, _ := txn.RGet(tbeg.Encode(), tend.Encode(), 0)
		for _, kv := range tkvs {
			key := &keys.TB{}
			key.Decode(kv.Key())
			res.Inc(key.TB, "tables")
		}

		// Get the table definitions
		vbeg := &keys.VW{KV: ast.KV, NS: ast.NS, DB: ast.DB, VW: keys.Prefix}
		vend := &keys.VW{KV: ast.KV, NS: ast.NS, DB: ast.DB, VW: keys.Suffix}
		vkvs, _ := txn.RGet(vbeg.Encode(), vend.Encode(), 0)
		for _, kv := range vkvs {
			key, val := &keys.VW{}, &sql.DefineViewStatement{}
			key.Decode(kv.Key())
			pack.Decode(kv.Val(), val)
			res.Inc(val, "views")
		}

		out = append(out, res.Data())

	} else {

		res := data.New()
		res.Object("rules")
		res.Array("fields")
		res.Array("indexes")

		// Get the rules definitions
		rbeg := &keys.RU{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: ast.What, RU: keys.Prefix}
		rend := &keys.RU{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: ast.What, RU: keys.Suffix}
		rkvs, _ := txn.RGet(rbeg.Encode(), rend.Encode(), 0)
		for _, kv := range rkvs {
			key, val := &keys.RU{}, &sql.DefineRulesStatement{}
			key.Decode(kv.Key())
			pack.Decode(kv.Val(), val)
			res.Set(val, "rules", fmt.Sprintf("%v", key.RU))
		}

		// Get the field definitions
		fbeg := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: ast.What, FD: keys.Prefix}
		fend := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: ast.What, FD: keys.Suffix}
		fkvs, _ := txn.RGet(fbeg.Encode(), fend.Encode(), 0)
		for _, kv := range fkvs {
			key, val := &keys.FD{}, &sql.DefineFieldStatement{}
			key.Decode(kv.Key())
			pack.Decode(kv.Val(), val)
			res.Inc(val, "fields")
		}

		// Get the field definitions
		ibeg := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: ast.What, IX: keys.Prefix}
		iend := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: ast.What, IX: keys.Suffix}
		ikvs, _ := txn.RGet(ibeg.Encode(), iend.Encode(), 0)
		for _, kv := range ikvs {
			key, val := &keys.IX{}, &sql.DefineIndexStatement{}
			key.Decode(kv.Key())
			pack.Decode(kv.Val(), val)
			res.Inc(val, "indexes")
		}

		out = append(out, res.Data())

	}

	return

}
