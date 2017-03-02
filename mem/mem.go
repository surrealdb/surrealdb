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

package mem

import (
	"fmt"
	"math"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/keys"
)

type Store struct {
	tx kvs.TX
	kv string
}

var invalid = fmt.Errorf("Does not exist")

// --------------------------------------------------

func New(tx kvs.TX) *Store {
	return &Store{tx: tx, kv: cnf.Settings.DB.Base}
}

// --------------------------------------------------

func (s *Store) AllNS() (out []*sql.DefineNamespaceStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NS{KV: s.kv, NS: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineNamespaceStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetNS(ns string) (val *sql.DefineNamespaceStatement, err error) {

	var kv kvs.KV

	key := &keys.NS{KV: s.kv, NS: ns}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineNamespaceStatement{}
	val.Decode(kv.Val())

	return

}

func (s *Store) AddNS(ns string) (err error) {

	key := &keys.NS{KV: s.kv, NS: ns}
	val := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ns)}
	s.tx.PutC(0, key.Encode(), val.Encode(), nil)

	return

}

// --------------------------------------------------

func (s *Store) AllNT(ns string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NT{KV: s.kv, NS: ns, TK: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTokenStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetNT(ns, tk string) (val *sql.DefineTokenStatement, err error) {

	var kv kvs.KV

	key := &keys.NT{KV: s.kv, NS: ns, TK: tk}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineTokenStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (s *Store) AllNU(ns string) (out []*sql.DefineLoginStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NU{KV: s.kv, NS: ns, US: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineLoginStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetNU(ns, us string) (val *sql.DefineLoginStatement, err error) {

	var kv kvs.KV

	key := &keys.NU{KV: s.kv, NS: ns, US: us}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineLoginStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (s *Store) AllDB(ns string) (out []*sql.DefineDatabaseStatement, err error) {

	var kvs []kvs.KV

	key := &keys.DB{KV: s.kv, NS: ns, DB: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineDatabaseStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetDB(ns, db string) (val *sql.DefineDatabaseStatement, err error) {

	var kv kvs.KV

	key := &keys.DB{KV: s.kv, NS: ns, DB: db}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineDatabaseStatement{}
	val.Decode(kv.Val())

	return

}

func (s *Store) AddDB(ns, db string) (err error) {

	err = s.AddNS(ns)

	key := &keys.DB{KV: s.kv, NS: ns, DB: db}
	val := &sql.DefineDatabaseStatement{Name: sql.NewIdent(db)}
	s.tx.PutC(0, key.Encode(), val.Encode(), nil)

	return

}

// --------------------------------------------------

func (s *Store) AllDT(ns, db string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.DT{KV: s.kv, NS: ns, DB: db, TK: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTokenStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetDT(ns, db, tk string) (val *sql.DefineTokenStatement, err error) {

	var kv kvs.KV

	key := &keys.DT{KV: s.kv, NS: ns, DB: db, TK: tk}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineTokenStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (s *Store) AllDU(ns, db string) (out []*sql.DefineLoginStatement, err error) {

	var kvs []kvs.KV

	key := &keys.DU{KV: s.kv, NS: ns, DB: db, US: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineLoginStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetDU(ns, db, us string) (val *sql.DefineLoginStatement, err error) {

	var kv kvs.KV

	key := &keys.DU{KV: s.kv, NS: ns, DB: db, US: us}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineLoginStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (s *Store) AllSC(ns, db string) (out []*sql.DefineScopeStatement, err error) {

	var kvs []kvs.KV

	key := &keys.SC{KV: s.kv, NS: ns, DB: db, SC: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineScopeStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetSC(ns, db, sc string) (val *sql.DefineScopeStatement, err error) {

	var kv kvs.KV

	key := &keys.SC{KV: s.kv, NS: ns, DB: db, SC: sc}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineScopeStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (s *Store) AllST(ns, db, sc string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.ST{KV: s.kv, NS: ns, DB: db, SC: sc, TK: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTokenStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetST(ns, db, sc, tk string) (val *sql.DefineTokenStatement, err error) {

	var kv kvs.KV

	key := &keys.ST{KV: s.kv, NS: ns, DB: db, SC: sc, TK: tk}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineTokenStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (s *Store) AllTB(ns, db string) (out []*sql.DefineTableStatement, err error) {

	var kvs []kvs.KV

	key := &keys.TB{KV: s.kv, NS: ns, DB: db, TB: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetTB(ns, db, tb string) (val *sql.DefineTableStatement, err error) {

	var kv kvs.KV

	key := &keys.TB{KV: s.kv, NS: ns, DB: db, TB: tb}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineTableStatement{}
	val.Decode(kv.Val())

	return

}

func (s *Store) AddTB(ns, db, tb string) (err error) {

	err = s.AddDB(ns, db)

	key := &keys.TB{KV: s.kv, NS: ns, DB: db, TB: tb}
	val := &sql.DefineTableStatement{What: sql.Tables{sql.NewTable(tb)}}
	s.tx.PutC(0, key.Encode(), val.Encode(), nil)

	return

}

// --------------------------------------------------

func (s *Store) AllFD(ns, db, tb string) (out []*sql.DefineFieldStatement, err error) {

	var kvs []kvs.KV

	key := &keys.FD{KV: s.kv, NS: ns, DB: db, TB: tb, FD: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineFieldStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetFD(ns, db, tb, fd string) (val *sql.DefineFieldStatement, err error) {

	var kv kvs.KV

	key := &keys.FD{KV: s.kv, NS: ns, DB: db, TB: tb, FD: fd}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineFieldStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (s *Store) AllIX(ns, db, tb string) (out []*sql.DefineIndexStatement, err error) {

	var kvs []kvs.KV

	key := &keys.IX{KV: s.kv, NS: ns, DB: db, TB: tb, IX: keys.Ignore}
	if kvs, err = s.tx.GetL(math.MaxInt64, key.Encode()); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineIndexStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (s *Store) GetIX(ns, db, tb, ix string) (val *sql.DefineIndexStatement, err error) {

	var kv kvs.KV

	key := &keys.IX{KV: s.kv, NS: ns, DB: db, TB: tb, IX: ix}
	if kv, err = s.tx.Get(math.MaxInt64, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, invalid
	}

	val = &sql.DefineIndexStatement{}
	val.Decode(kv.Val())

	return

}
