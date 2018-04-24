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
	lock  sync.RWMutex
	data  map[string]interface{}
	locks struct {
		ns sync.RWMutex
		db sync.RWMutex
		tb sync.RWMutex
	}
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

func (c *Cache) get(key keys.Key) (out interface{}, ok bool) {
	c.lock.RLock()
	out, ok = c.data[key.String()]
	c.lock.RUnlock()
	return
}

func (c *Cache) put(key keys.Key, val interface{}) {
	c.lock.Lock()
	c.data[key.String()] = val
	c.lock.Unlock()
}

func (c *Cache) del(key keys.Key) {
	c.lock.Lock()
	delete(c.data, key.String())
	c.lock.Unlock()
}

// --------------------------------------------------

func (c *Cache) AllNS() (out []*sql.DefineNamespaceStatement, err error) {

	var kvs []kvs.KV

	c.locks.ns.RLock()
	defer c.locks.ns.RUnlock()

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.DefineNamespaceStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineNamespaceStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetNS(ns string) (val *sql.DefineNamespaceStatement, err error) {

	var kv kvs.KV

	c.locks.ns.RLock()
	defer c.locks.ns.RUnlock()

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: ns}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineNamespaceStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorNSNotFound
	}

	val = &sql.DefineNamespaceStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) AddNS(ns string) (val *sql.DefineNamespaceStatement, err error) {

	var kv kvs.KV

	c.locks.ns.Lock()
	defer c.locks.ns.Unlock()

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: ns}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineNamespaceStatement), nil
	}

	if kv, _ = c.TX.Get(0, key.Encode()); kv.Exi() {
		val = &sql.DefineNamespaceStatement{}
		val.Decode(kv.Val())
		c.put(key, val)
		return
	}

	val = &sql.DefineNamespaceStatement{Name: sql.NewIdent(ns)}
	c.TX.PutC(0, key.Encode(), val.Encode(), nil)

	c.put(key, val)

	return

}

func (c *Cache) DelNS(ns string) {

	c.del(&keys.NS{KV: cnf.Settings.DB.Base, NS: keys.Ignore})

	c.del(&keys.NS{KV: cnf.Settings.DB.Base, NS: ns})

}

// --------------------------------------------------

func (c *Cache) AllNT(ns string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NT{KV: cnf.Settings.DB.Base, NS: ns, TK: keys.Ignore}
	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
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
	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
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

	var kvs []kvs.KV

	c.locks.db.RLock()
	defer c.locks.db.RUnlock()

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.DefineDatabaseStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineDatabaseStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetDB(ns, db string) (val *sql.DefineDatabaseStatement, err error) {

	var kv kvs.KV

	c.locks.db.RLock()
	defer c.locks.db.RUnlock()

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: db}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineDatabaseStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorDBNotFound
	}

	val = &sql.DefineDatabaseStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) AddDB(ns, db string) (val *sql.DefineDatabaseStatement, err error) {

	if _, err = c.AddNS(ns); err != nil {
		return
	}

	var kv kvs.KV

	c.locks.db.Lock()
	defer c.locks.db.Unlock()

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: db}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineDatabaseStatement), nil
	}

	if kv, _ = c.TX.Get(0, key.Encode()); kv.Exi() {
		val = &sql.DefineDatabaseStatement{}
		val.Decode(kv.Val())
		c.put(key, val)
		return
	}

	val = &sql.DefineDatabaseStatement{Name: sql.NewIdent(db)}
	c.TX.PutC(0, key.Encode(), val.Encode(), nil)

	c.put(key, val)

	return

}

func (c *Cache) DelDB(ns, db string) {

	c.del(&keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: keys.Ignore})

	c.del(&keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: db})

}

// --------------------------------------------------

func (c *Cache) AllDT(ns, db string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.DT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TK: keys.Ignore}
	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
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
	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
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
	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
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
	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
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

	var kvs []kvs.KV

	c.locks.tb.RLock()
	defer c.locks.tb.RUnlock()

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.DefineTableStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetTB(ns, db, tb string) (val *sql.DefineTableStatement, err error) {

	var kv kvs.KV

	c.locks.tb.RLock()
	defer c.locks.tb.RUnlock()

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorTBNotFound
	}

	val = &sql.DefineTableStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) AddTB(ns, db, tb string) (val *sql.DefineTableStatement, err error) {

	if _, err = c.AddDB(ns, db); err != nil {
		return
	}

	var kv kvs.KV

	c.locks.tb.Lock()
	defer c.locks.tb.Unlock()

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	if kv, _ = c.TX.Get(0, key.Encode()); kv.Exi() {
		val = &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		c.put(key, val)
		return
	}

	val = &sql.DefineTableStatement{Name: sql.NewIdent(tb)}
	c.TX.PutC(0, key.Encode(), val.Encode(), nil)

	c.put(key, val)

	return

}

func (c *Cache) DelTB(ns, db, tb string) {

	c.del(&keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: keys.Ignore})

	c.del(&keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb})

}

// --------------------------------------------------

func (c *Cache) AllEV(ns, db, tb string) (out []*sql.DefineEventStatement, err error) {

	var kvs []kvs.KV

	key := &keys.EV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, EV: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.DefineEventStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineEventStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetEV(ns, db, tb, ev string) (val *sql.DefineEventStatement, err error) {

	var kv kvs.KV

	key := &keys.EV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, EV: ev}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineEventStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorEVNotFound
	}

	val = &sql.DefineEventStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) DelEV(ns, db, tb, ev string) {

	c.del(&keys.EV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, EV: keys.Ignore})

	c.del(&keys.EV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, EV: ev})

}

// --------------------------------------------------

func (c *Cache) AllFD(ns, db, tb string) (out []*sql.DefineFieldStatement, err error) {

	var kvs []kvs.KV

	key := &keys.FD{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FD: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.DefineFieldStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineFieldStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetFD(ns, db, tb, fd string) (val *sql.DefineFieldStatement, err error) {

	var kv kvs.KV

	key := &keys.FD{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FD: fd}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineFieldStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorFDNotFound
	}

	val = &sql.DefineFieldStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) DelFD(ns, db, tb, fd string) {

	c.del(&keys.FD{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FD: keys.Ignore})

	c.del(&keys.FD{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FD: fd})

}

// --------------------------------------------------

func (c *Cache) AllIX(ns, db, tb string) (out []*sql.DefineIndexStatement, err error) {

	var kvs []kvs.KV

	key := &keys.IX{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, IX: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.DefineIndexStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineIndexStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetIX(ns, db, tb, ix string) (val *sql.DefineIndexStatement, err error) {

	var kv kvs.KV

	key := &keys.IX{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, IX: ix}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineIndexStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorIXNotFound
	}

	val = &sql.DefineIndexStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) DelIX(ns, db, tb, ix string) {

	c.del(&keys.IX{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, IX: keys.Ignore})

	c.del(&keys.IX{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, IX: ix})

}

// --------------------------------------------------

func (c *Cache) AllFT(ns, db, tb string) (out []*sql.DefineTableStatement, err error) {

	var kvs []kvs.KV

	key := &keys.FT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FT: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.DefineTableStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetFT(ns, db, tb, ft string) (val *sql.DefineTableStatement, err error) {

	var kv kvs.KV

	key := &keys.FT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FT: ft}

	if out, ok := c.get(key); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorFTNotFound
	}

	val = &sql.DefineTableStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) DelFT(ns, db, tb, ft string) {

	c.del(&keys.FT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FT: keys.Ignore})

	c.del(&keys.FT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FT: ft})

}

// --------------------------------------------------

func (c *Cache) AllLV(ns, db, tb string) (out []*sql.LiveStatement, err error) {

	var kvs []kvs.KV

	key := &keys.LV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, LV: keys.Ignore}

	if out, ok := c.get(key); ok {
		return out.([]*sql.LiveStatement), nil
	}

	if kvs, err = c.TX.GetP(0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.LiveStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	c.put(key, out)

	return

}

func (c *Cache) GetLV(ns, db, tb, lv string) (val *sql.LiveStatement, err error) {

	var kv kvs.KV

	key := &keys.LV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, LV: lv}

	if out, ok := c.get(key); ok {
		return out.(*sql.LiveStatement), nil
	}

	if kv, err = c.TX.Get(0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorLVNotFound
	}

	val = &sql.LiveStatement{}
	val.Decode(kv.Val())

	c.put(key, val)

	return

}

func (c *Cache) DelLV(ns, db, tb, lv string) {

	c.del(&keys.LV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, LV: keys.Ignore})

	c.del(&keys.LV{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, LV: lv})

}
