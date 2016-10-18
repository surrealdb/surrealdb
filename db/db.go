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
	"time"

	"runtime/debug"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"

	_ "github.com/abcum/surreal/kvs/boltdb"
	_ "github.com/abcum/surreal/kvs/mysql"
	_ "github.com/abcum/surreal/kvs/pgsql"
)

type Response struct {
	Time   string      `codec:"time,omitempty"`
	Status interface{} `codec:"status,omitempty"`
	Detail interface{} `codec:"detail,omitempty"`
	Result interface{} `codec:"result,omitempty"`
}

var db *kvs.DB

// Setup sets up the connection with the data layer
func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("db").Infof("Starting database")

	db, err = kvs.New(opts)

	return

}

// Exit shuts down the connection with the data layer
func Exit() {

	log.WithPrefix("db").Infof("Gracefully shutting down database")

	db.Close()

}

// Execute parses the query and executes it against the data layer
func Execute(ctx *fibre.Context, txt interface{}, vars map[string]interface{}) (out []*Response, err error) {

	ast, err := sql.Parse(ctx, txt, vars)
	if err != nil {
		return
	}

	chn := make(chan interface{})

	go execute(ctx, ast, chn)

	for msg := range chn {
		switch res := msg.(type) {
		case error:
			return nil, res
		case *Response:
			out = append(out, res)
		}
	}

	return

}

func status(e error) interface{} {
	switch e.(type) {
	default:
		return "OK"
	case *kvs.DBError:
		return "ERR_DB"
	case *kvs.TXError:
		return "ERR_TX"
	case *kvs.CKError:
		return "ERR_CK"
	case *kvs.KVError:
		return "ERR_KV"
	case error:
		return "ERR"
	}
}

func detail(e error) interface{} {
	switch err := e.(type) {
	default:
		return nil
	case error:
		return err.Error()
	}
}

func writable(cur kvs.TX, tmp bool) (txn kvs.TX, err error, loc bool) {
	if cur == nil {
		cur, err = db.Txn(true)
	}
	return cur, err, tmp
}

func readable(cur kvs.TX, tmp bool) (txn kvs.TX, err error, loc bool) {
	if cur == nil {
		cur, err = db.Txn(false)
	}
	return cur, err, tmp
}

func execute(ctx *fibre.Context, ast *sql.Query, chn chan<- interface{}) {

	var txn kvs.TX

	defer func() {
		if txn != nil {
			txn.Cancel()
		}
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				fmt.Printf("%s", debug.Stack())
				chn <- err
			}
		}
		close(chn)
	}()

	for _, s := range ast.Statements {

		var loc bool
		var err error
		var res []interface{}

		now := time.Now()

		switch s.(type) {

		case *sql.UseStatement:
			continue
		case *sql.BeginStatement:
			break
		case *sql.CancelStatement:
			break
		case *sql.CommitStatement:
			break
		case *sql.InfoStatement:
			txn, err, loc = readable(txn, txn == nil)
		default:
			txn, err, loc = writable(txn, txn == nil)
		}

		if err != nil {
			chn <- err
		}

		switch stm := s.(type) {

		case *sql.CommitStatement:
			txn.Commit()
			txn = nil
			continue

		case *sql.CancelStatement:
			txn.Cancel()
			txn = nil
			continue

		case *sql.BeginStatement:
			txn, err, loc = writable(txn, false)
			continue

		case *sql.InfoStatement:
			res, err = executeInfoStatement(txn, stm)

		case *sql.SelectStatement:
			res, err = executeSelectStatement(txn, stm)
		case *sql.CreateStatement:
			res, err = executeCreateStatement(txn, stm)
		case *sql.UpdateStatement:
			res, err = executeUpdateStatement(txn, stm)
		case *sql.ModifyStatement:
			res, err = executeModifyStatement(txn, stm)
		case *sql.DeleteStatement:
			res, err = executeDeleteStatement(txn, stm)
		case *sql.RelateStatement:
			res, err = executeRelateStatement(txn, stm)

		case *sql.DefineTableStatement:
			res, err = executeDefineTableStatement(txn, stm)
		case *sql.RemoveTableStatement:
			res, err = executeRemoveTableStatement(txn, stm)

		case *sql.DefineRulesStatement:
			res, err = executeDefineRulesStatement(txn, stm)
		case *sql.RemoveRulesStatement:
			res, err = executeRemoveRulesStatement(txn, stm)

		case *sql.DefineFieldStatement:
			res, err = executeDefineFieldStatement(txn, stm)
		case *sql.RemoveFieldStatement:
			res, err = executeRemoveFieldStatement(txn, stm)

		case *sql.DefineIndexStatement:
			res, err = executeDefineIndexStatement(txn, stm)
		case *sql.RemoveIndexStatement:
			res, err = executeRemoveIndexStatement(txn, stm)

		}

		if err != nil {
			chn <- err
		}

		if loc {
			if err != nil {
				txn.Cancel()
			} else {
				txn.Commit()
			}
			txn = nil
		}

		chn <- &Response{
			Time:   time.Since(now).String(),
			Status: status(err),
			Detail: detail(err),
			Result: append([]interface{}{}, res...),
		}

	}

}
