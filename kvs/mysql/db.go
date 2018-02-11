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
	"io"

	"context"

	"database/sql"

	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
)

type DB struct {
	pntr *sql.DB
}

func (db *DB) Begin(ctx context.Context, writable bool) (txn kvs.TX, err error) {
	var pntr *sql.Tx
	if pntr, err = db.pntr.BeginTx(ctx, db.opt(writable)); err != nil {
		log.WithPrefix("kvs").Errorln(err)
		err = &kvs.DBError{Err: err}
		return
	}
	return &TX{pntr: pntr}, err
}

func (db *DB) Import(r io.Reader) (err error) {
	return nil
}

func (db *DB) Export(w io.Writer) (err error) {
	return nil
}

func (db *DB) Close() (err error) {
	return db.pntr.Close()
}

func (db *DB) opt(writable bool) *sql.TxOptions {
	return &sql.TxOptions{
		ReadOnly: !writable,
	}
}
