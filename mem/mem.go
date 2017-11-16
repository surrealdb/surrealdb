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
	"sync"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/keys"
)

type Cache struct {
	kvs.TX
	lock sync.RWMutex
	data map[string]interface{}
}

func New() (c *Cache) {
	return &Cache{
		data: make(map[string]interface{}),
	}
}

func NewWithTX(tx kvs.TX) (c *Cache) {
	return &Cache{
		TX:   tx,
		data: make(map[string]interface{}),
	}
}

func (c *Cache) Reset() {
	c.TX = nil
}

func (c *Cache) get(idx string) (out interface{}, ok bool) {
	c.lock.RLock()
	out, ok = c.data[idx]
	c.lock.RUnlock()
	return
}

func (c *Cache) put(idx string, val interface{}) {
	c.lock.Lock()
	c.data[idx] = val
	c.lock.Unlock()
}

func (c *Cache) del(idx string) {
	c.lock.Lock()
	delete(c.data, idx)
	c.lock.Unlock()
}

// --------------------------------------------------

func (c *Cache) AllNS() (out []*sql.DefineNamespaceStatement, err error) {

	idx := (&keys.KV{}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.DefineNamespaceStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineNamespaceStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetNS(ns string) (val *sql.DefineNamespaceStatement, err error) {

	idx := (&keys.NS{NS: ns}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineNamespaceStatement), nil
	}

	var kv kvs.KV

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: ns}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorNSNotFound
	}

	val = &sql.DefineNamespaceStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) AddNS(ns string) (*sql.DefineNamespaceStatement, error) {

	idx := (&keys.NS{NS: ns}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineNamespaceStatement), nil
	}

	if out, err := c.GetNS(ns); err == nil {
		return out, nil
	}

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: ns}
	val := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ns)}
	if _, err := c.TX.PutC(0, key.Encode(), val.Encode(), nil); err != nil {
		return nil, err
	}

	c.put(idx, val)

	return val, nil

}

func (c *Cache) DelNS(ns string) {

	c.del((&keys.NS{NS: keys.Ignore}).String())

	c.del((&keys.NS{NS: ns}).String())

	return

}

// --------------------------------------------------

func (c *Cache) AllNT(ns string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NT{KV: cnf.Settings.DB.Base, NS: ns, TK: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTokenStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (c *Cache) GetNT(ns, tk string) (val *sql.DefineTokenStatement, err error) {

	var kv kvs.KV

	key := &keys.NT{KV: cnf.Settings.DB.Base, NS: ns, TK: tk}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorNTNotFound
	}

	val = &sql.DefineTokenStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (c *Cache) AllNU(ns string) (out []*sql.DefineLoginStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NU{KV: cnf.Settings.DB.Base, NS: ns, US: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineLoginStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (c *Cache) GetNU(ns, us string) (val *sql.DefineLoginStatement, err error) {

	var kv kvs.KV

	key := &keys.NU{KV: cnf.Settings.DB.Base, NS: ns, US: us}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorNUNotFound
	}

	val = &sql.DefineLoginStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (c *Cache) AllDB(ns string) (out []*sql.DefineDatabaseStatement, err error) {

	idx := (&keys.DB{NS: ns, DB: keys.Ignore}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.DefineDatabaseStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineDatabaseStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetDB(ns, db string) (val *sql.DefineDatabaseStatement, err error) {

	idx := (&keys.DB{NS: ns, DB: db}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineDatabaseStatement), nil
	}

	var kv kvs.KV

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: db}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorDBNotFound
	}

	val = &sql.DefineDatabaseStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) AddDB(ns, db string) (*sql.DefineDatabaseStatement, error) {

	idx := (&keys.DB{NS: ns, DB: db}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineDatabaseStatement), nil
	}

	if out, err := c.GetDB(ns, db); err == nil {
		return out, nil
	}

	if _, err := c.AddNS(ns); err != nil {
		return nil, err
	}

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: db}
	val := &sql.DefineDatabaseStatement{Name: sql.NewIdent(db)}
	if _, err := c.TX.PutC(0, key.Encode(), val.Encode(), nil); err != nil {
		return nil, err
	}

	c.put(idx, val)

	return val, nil

}

func (c *Cache) DelDB(ns, db string) {

	c.del((&keys.DB{NS: ns, DB: keys.Ignore}).String())

	c.del((&keys.DB{NS: ns, DB: db}).String())

	return

}

// --------------------------------------------------

func (c *Cache) AllDT(ns, db string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.DT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TK: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTokenStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (c *Cache) GetDT(ns, db, tk string) (val *sql.DefineTokenStatement, err error) {

	var kv kvs.KV

	key := &keys.DT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TK: tk}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorDTNotFound
	}

	val = &sql.DefineTokenStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (c *Cache) AllDU(ns, db string) (out []*sql.DefineLoginStatement, err error) {

	var kvs []kvs.KV

	key := &keys.DU{KV: cnf.Settings.DB.Base, NS: ns, DB: db, US: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineLoginStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (c *Cache) GetDU(ns, db, us string) (val *sql.DefineLoginStatement, err error) {

	var kv kvs.KV

	key := &keys.DU{KV: cnf.Settings.DB.Base, NS: ns, DB: db, US: us}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorDUNotFound
	}

	val = &sql.DefineLoginStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (c *Cache) AllSC(ns, db string) (out []*sql.DefineScopeStatement, err error) {

	var kvs []kvs.KV

	key := &keys.SC{KV: cnf.Settings.DB.Base, NS: ns, DB: db, SC: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineScopeStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (c *Cache) GetSC(ns, db, sc string) (val *sql.DefineScopeStatement, err error) {

	var kv kvs.KV

	key := &keys.SC{KV: cnf.Settings.DB.Base, NS: ns, DB: db, SC: sc}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorSCNotFound
	}

	val = &sql.DefineScopeStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (c *Cache) AllST(ns, db, sc string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.ST{KV: cnf.Settings.DB.Base, NS: ns, DB: db, SC: sc, TK: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTokenStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (c *Cache) GetST(ns, db, sc, tk string) (val *sql.DefineTokenStatement, err error) {

	var kv kvs.KV

	key := &keys.ST{KV: cnf.Settings.DB.Base, NS: ns, DB: db, SC: sc, TK: tk}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorSTNotFound
	}

	val = &sql.DefineTokenStatement{}
	val.Decode(kv.Val())

	return

}

// --------------------------------------------------

func (c *Cache) AllTB(ns, db string) (out []*sql.DefineTableStatement, err error) {

	idx := (&keys.TB{NS: ns, DB: db, TB: keys.Ignore}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.DefineTableStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetTB(ns, db, tb string) (val *sql.DefineTableStatement, err error) {

	idx := (&keys.TB{NS: ns, DB: db, TB: tb}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	var kv kvs.KV

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorTBNotFound
	}

	val = &sql.DefineTableStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) AddTB(ns, db, tb string) (*sql.DefineTableStatement, error) {

	// var exi bool

	idx := (&keys.TB{NS: ns, DB: db, TB: tb}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	if out, err := c.GetTB(ns, db, tb); err == nil {
		return out, nil
	}

	if _, err := c.AddDB(ns, db); err != nil {
		return nil, err
	}

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb}
	val := &sql.DefineTableStatement{Name: sql.NewIdent(tb)}
	if _, err := c.TX.PutC(0, key.Encode(), val.Encode(), nil); err != nil {
		return nil, err
	}

	c.put(idx, val)

	return val, nil

}

func (c *Cache) DelTB(ns, db, tb string) {

	c.del((&keys.TB{NS: ns, DB: db, TB: keys.Ignore}).String())

	c.del((&keys.TB{NS: ns, DB: db, TB: tb}).String())

	return

}

// --------------------------------------------------

func (c *Cache) AllEV(ns, db, tb string) (out []*sql.DefineEventStatement, err error) {

	idx := (&keys.EV{NS: ns, DB: db, TB: tb, EV: keys.Ignore}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.DefineEventStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.EV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, EV: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineEventStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetEV(ns, db, tb, ev string) (val *sql.DefineEventStatement, err error) {

	idx := (&keys.EV{NS: ns, DB: db, TB: tb, EV: ev}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineEventStatement), nil
	}

	var kv kvs.KV

	key := &keys.EV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, EV: ev}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorEVNotFound
	}

	val = &sql.DefineEventStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) DelEV(ns, db, tb, ev string) {

	c.del((&keys.EV{NS: ns, DB: db, TB: tb, EV: keys.Ignore}).String())

	c.del((&keys.EV{NS: ns, DB: db, TB: tb, EV: ev}).String())

	return

}

// --------------------------------------------------

func (c *Cache) AllFD(ns, db, tb string) (out []*sql.DefineFieldStatement, err error) {

	idx := (&keys.FD{NS: ns, DB: db, TB: tb, FD: keys.Ignore}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.DefineFieldStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.FD{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FD: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineFieldStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetFD(ns, db, tb, fd string) (val *sql.DefineFieldStatement, err error) {

	idx := (&keys.FD{NS: ns, DB: db, TB: tb, FD: fd}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineFieldStatement), nil
	}

	var kv kvs.KV

	key := &keys.FD{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FD: fd}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorFDNotFound
	}

	val = &sql.DefineFieldStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) DelFD(ns, db, tb, fd string) {

	c.del((&keys.FD{NS: ns, DB: db, TB: tb, FD: keys.Ignore}).String())

	c.del((&keys.FD{NS: ns, DB: db, TB: tb, FD: fd}).String())

	return

}

// --------------------------------------------------

func (c *Cache) AllIX(ns, db, tb string) (out []*sql.DefineIndexStatement, err error) {

	idx := (&keys.IX{NS: ns, DB: db, TB: tb, IX: keys.Ignore}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.DefineIndexStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.IX{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, IX: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineIndexStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetIX(ns, db, tb, ix string) (val *sql.DefineIndexStatement, err error) {

	idx := (&keys.IX{NS: ns, DB: db, TB: tb, IX: ix}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineIndexStatement), nil
	}

	var kv kvs.KV

	key := &keys.IX{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, IX: ix}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorIXNotFound
	}

	val = &sql.DefineIndexStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) DelIX(ns, db, tb, ix string) {

	c.del((&keys.IX{NS: ns, DB: db, TB: tb, IX: keys.Ignore}).String())

	c.del((&keys.IX{NS: ns, DB: db, TB: tb, IX: ix}).String())

	return

}

// --------------------------------------------------

func (c *Cache) AllFT(ns, db, tb string) (out []*sql.DefineTableStatement, err error) {

	idx := (&keys.FT{NS: ns, DB: db, TB: tb, FT: keys.Ignore}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.DefineTableStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.FT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FT: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetFT(ns, db, tb, ft string) (val *sql.DefineTableStatement, err error) {

	idx := (&keys.FT{NS: ns, DB: db, TB: tb, FT: ft}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	var kv kvs.KV

	key := &keys.FT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FT: ft}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorFTNotFound
	}

	val = &sql.DefineTableStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) DelFT(ns, db, tb, ft string) {

	c.del((&keys.FT{NS: ns, DB: db, TB: tb, FT: keys.Ignore}).String())

	c.del((&keys.FT{NS: ns, DB: db, TB: tb, FT: ft}).String())

	return

}

// --------------------------------------------------

func (c *Cache) AllLV(ns, db, tb string) (out []*sql.LiveStatement, err error) {

	idx := (&keys.LV{NS: ns, DB: db, TB: tb, LV: keys.Ignore}).String()

	if out, ok := c.get(idx); ok {
		return out.([]*sql.LiveStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.LV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, LV: keys.Ignore}
	if kvs, err = c.TX.GetL(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.LiveStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(idx, out)

	return

}

func (c *Cache) GetLV(ns, db, tb, lv string) (val *sql.LiveStatement, err error) {

	idx := (&keys.LV{NS: ns, DB: db, TB: tb, LV: lv}).String()

	if out, ok := c.get(idx); ok {
		return out.(*sql.LiveStatement), nil
	}

	var kv kvs.KV

	key := &keys.LV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, LV: lv}
	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorLVNotFound
	}

	val = &sql.LiveStatement{}
	val.Decode(kv.Val())

	c.put(idx, val)

	return

}

func (c *Cache) DelLV(ns, db, tb, lv string) {

	c.del((&keys.LV{NS: ns, DB: db, TB: tb, LV: keys.Ignore}).String())

	c.del((&keys.LV{NS: ns, DB: db, TB: tb, LV: lv}).String())

	return

}
