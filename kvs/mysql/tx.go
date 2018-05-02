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

package mysql

import (
	"math"
	"sync"

	"context"

	"database/sql"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
)

type TX struct {
	done bool
	pntr *sql.Tx
	lock sync.Mutex
	stmt struct {
		clr  *sql.Stmt
		clrP *sql.Stmt
		clrR *sql.Stmt
		get  *sql.Stmt
		getP *sql.Stmt
		getR *sql.Stmt
		del  *sql.Stmt
		delP *sql.Stmt
		delR *sql.Stmt
		put  *sql.Stmt
		putN *sql.Stmt
	}
}

const maximum = math.MaxUint64

func dec(src []byte) (dst []byte, err error) {
	if dst, err = decrypt(cnf.Settings.DB.Key, src); err != nil {
		return nil, &kvs.DBError{}
	}
	return
}

func enc(src []byte) (dst []byte, err error) {
	if dst, err = encrypt(cnf.Settings.DB.Key, src); err != nil {
		return nil, &kvs.DBError{}
	}
	return
}

func one(res *sql.Rows, err error) (kvs.KV, error) {

	switch err {
	case nil:
		break
	default:
		return nil, &kvs.DBError{}
	}

	defer res.Close()

	var out = &KV{}

	for res.Next() {
		err = res.Scan(&out.ver, &out.key, &out.val)
		if err != nil {
			return nil, &kvs.DBError{}
		}
		out.val, err = dec(out.val)
		if err != nil {
			return nil, &kvs.DBError{}
		}
	}

	if err = res.Err(); err != nil {
		return nil, &kvs.DBError{}
	}

	return out, err

}

func many(res *sql.Rows, err error) ([]kvs.KV, error) {

	switch err {
	case nil:
		break
	default:
		return nil, &kvs.DBError{}
	}

	defer res.Close()

	var out []kvs.KV

	for res.Next() {
		kv := &KV{}
		err = res.Scan(&kv.ver, &kv.key, &kv.val)
		if err != nil {
			return nil, &kvs.DBError{}
		}
		kv.val, err = dec(kv.val)
		if err != nil {
			return nil, &kvs.DBError{}
		}
		if kv.val != nil {
			out = append(out, kv)
		}
	}

	if err = res.Err(); err != nil {
		return nil, &kvs.DBError{}
	}

	return out, err

}

func (tx *TX) Closed() bool {
	return tx.done
}

func (tx *TX) Cancel() error {
	tx.done = true
	return tx.pntr.Rollback()
}

func (tx *TX) Commit() error {
	tx.done = true
	return tx.pntr.Commit()
}

func (tx *TX) Clr(ctx context.Context, key []byte) (kvs.KV, error) {

	var err error
	var res *sql.Rows

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.clr == nil {
		tx.stmt.clr, _ = tx.pntr.PrepareContext(ctx, sqlClr)
	}

	res, err = tx.stmt.clr.QueryContext(ctx, key)

	return one(res, err)

}

func (tx *TX) ClrP(ctx context.Context, key []byte, max uint64) ([]kvs.KV, error) {

	var err error
	var res *sql.Rows

	if max == 0 {
		max = maximum
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.clrP == nil {
		tx.stmt.clrP, _ = tx.pntr.PrepareContext(ctx, sqlClrP)
	}

	res, err = tx.stmt.clrP.QueryContext(ctx, key, max)

	return many(res, err)

}

func (tx *TX) ClrR(ctx context.Context, beg []byte, end []byte, max uint64) ([]kvs.KV, error) {

	var err error
	var res *sql.Rows

	if max == 0 {
		max = maximum
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.clrR == nil {
		tx.stmt.clrR, _ = tx.pntr.PrepareContext(ctx, sqlClrR)
	}

	res, err = tx.stmt.clrR.QueryContext(ctx, beg, end, max)

	return many(res, err)

}

func (tx *TX) Get(ctx context.Context, ver int64, key []byte) (kvs.KV, error) {

	var err error
	var res *sql.Rows

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.get == nil {
		tx.stmt.get, _ = tx.pntr.PrepareContext(ctx, sqlGet)
	}

	res, err = tx.stmt.get.QueryContext(ctx, ver, key)

	return one(res, err)

}

func (tx *TX) GetP(ctx context.Context, ver int64, key []byte, max uint64) ([]kvs.KV, error) {

	var err error
	var res *sql.Rows

	if max == 0 {
		max = maximum
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.getP == nil {
		tx.stmt.getP, _ = tx.pntr.PrepareContext(ctx, sqlGetP)
	}

	res, err = tx.stmt.getP.QueryContext(ctx, ver, key, max)

	return many(res, err)

}

func (tx *TX) GetR(ctx context.Context, ver int64, beg []byte, end []byte, max uint64) ([]kvs.KV, error) {

	var err error
	var res *sql.Rows

	if max == 0 {
		max = maximum
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.getR == nil {
		tx.stmt.getR, _ = tx.pntr.PrepareContext(ctx, sqlGetR)
	}

	res, err = tx.stmt.getR.QueryContext(ctx, ver, beg, end, max)

	return many(res, err)

}

func (tx *TX) Del(ctx context.Context, ver int64, key []byte) (kvs.KV, error) {

	var err error
	var res *sql.Rows

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.del == nil {
		tx.stmt.del, _ = tx.pntr.PrepareContext(ctx, sqlDel)
	}

	res, err = tx.stmt.del.QueryContext(ctx, ver, key)

	return one(res, err)

}

func (tx *TX) DelC(ctx context.Context, ver int64, key []byte, exp []byte) (kvs.KV, error) {

	var err error
	var now kvs.KV
	var res *sql.Rows

	tx.lock.Lock()
	defer tx.lock.Unlock()

	// Get the item at the key

	if tx.stmt.get == nil {
		tx.stmt.get, _ = tx.pntr.PrepareContext(ctx, sqlGet)
	}

	res, err = tx.stmt.get.QueryContext(ctx, ver, key)
	if err != nil {
		return nil, err
	}

	now, err = one(res, err)
	if err != nil {
		return nil, err
	}

	// Check if the values match

	if !alter(now.Val(), exp) {
		return nil, ErrTxNotExpectedValue
	}

	// If they match then delete

	if tx.stmt.del == nil {
		tx.stmt.del, _ = tx.pntr.PrepareContext(ctx, sqlDel)
	}

	res, err = tx.stmt.del.QueryContext(ctx, ver, key)

	return one(res, err)

}

func (tx *TX) DelP(ctx context.Context, ver int64, key []byte, max uint64) ([]kvs.KV, error) {

	var err error
	var res *sql.Rows

	if max == 0 {
		max = maximum
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.delP == nil {
		tx.stmt.delP, _ = tx.pntr.PrepareContext(ctx, sqlDelP)
	}

	res, err = tx.stmt.delP.QueryContext(ctx, ver, key, max)

	return many(res, err)

}

func (tx *TX) DelR(ctx context.Context, ver int64, beg []byte, end []byte, max uint64) ([]kvs.KV, error) {

	var err error
	var res *sql.Rows

	if max == 0 {
		max = maximum
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.delR == nil {
		tx.stmt.delR, _ = tx.pntr.PrepareContext(ctx, sqlDelR)
	}

	res, err = tx.stmt.delR.QueryContext(ctx, ver, beg, end, max)

	return many(res, err)

}

func (tx *TX) Put(ctx context.Context, ver int64, key []byte, val []byte) (kvs.KV, error) {

	var err error
	var res *sql.Rows

	val, err = enc(val)
	if err != nil {
		return nil, err
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.stmt.put == nil {
		tx.stmt.put, _ = tx.pntr.PrepareContext(ctx, sqlPut)
	}

	res, err = tx.stmt.put.QueryContext(ctx, ver, key, val, val)

	return one(res, err)

}

func (tx *TX) PutC(ctx context.Context, ver int64, key []byte, val []byte, exp []byte) (kvs.KV, error) {

	var err error
	var now kvs.KV
	var res *sql.Rows

	val, err = enc(val)
	if err != nil {
		return nil, err
	}

	switch exp {

	case nil:

		if tx.stmt.putN == nil {
			tx.stmt.putN, _ = tx.pntr.PrepareContext(ctx, sqlPutN)
		}

		res, err = tx.stmt.putN.QueryContext(ctx, ver, key, val)

		return one(res, err)

	default:

		// Get the item at the key

		if tx.stmt.get == nil {
			tx.stmt.get, _ = tx.pntr.PrepareContext(ctx, sqlGet)
		}

		res, err = tx.stmt.get.QueryContext(ctx, ver, key)
		if err != nil {
			return nil, err
		}

		now, err = one(res, err)
		if err != nil {
			return nil, err
		}

		// Check if the values match

		if !check(now.Val(), exp) {
			return nil, ErrTxNotExpectedValue
		}

		// If they match then delete

		if tx.stmt.del == nil {
			tx.stmt.put, _ = tx.pntr.PrepareContext(ctx, sqlPut)
		}

		res, err = tx.stmt.put.QueryContext(ctx, ver, key, val, val)

		return one(res, err)

	}

}
