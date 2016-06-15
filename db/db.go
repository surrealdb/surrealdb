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

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"
)

type Response struct {
	Time   string      `json:"time,omitempty"`
	Status interface{} `json:"status,omitempty"`
	Detail interface{} `json:"detail,omitempty"`
	Result interface{} `json:"result,omitempty"`
}

var db *kvs.DB

// Setup sets up the connection with the data layer
func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("db").Infof("Starting database at %s", opts.DB.Path)

	db, err = kvs.New(opts.DB.Path)

	/*ticker := time.NewTicker(5 * time.Second)
	quit := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				t := time.Now()
				n := fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d-%d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
				p := fmt.Sprintf("dev/%s.backup.db", n)
				db.Save(p)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()*/

	return

}

// Exit shuts down the connection with the data layer
func Exit() {

	log.WithPrefix("db").Infof("Gracefully shutting down database")

	db.Close()

}

func Prepare(sql string, param ...interface{}) string {

	return fmt.Sprintf(sql, param...)

}

// Execute parses the query and executes it against the data layer
func Execute(ctx *fibre.Context, txt interface{}) (out []interface{}, err error) {

	ast, err := sql.Parse(ctx, txt)
	if err != nil {
		return
	}

	chn := make(chan interface{})

	go execute(ctx, ast, chn)

	for res := range chn {
		out = append(out, res)
	}

	return

}

func status(e error) interface{} {
	switch e.(type) {
	default:
		return "OK"
	case error:
		return "ERR"
	case *kvs.DBError:
		return "ERR_DB"
	case *kvs.TXError:
		return "ERR_DB"
	case *kvs.CKError:
		return "ERR_CK"
	case *kvs.KVError:
		return "ERR_EXISTS"
	}
}

func detail(e error) interface{} {
	switch err := e.(type) {
	default:
		return nil
	case error:
		return err.Error()
	case *kvs.DBError:
		return "A database error occured"
	case *kvs.TXError:
		return "A database error occured"
	case *kvs.CKError:
		return "A cipherkey error occured"
	case *kvs.KVError:
		return "The item already exists"
	}
}

func execute(ctx *fibre.Context, ast *sql.Query, chn chan interface{}) {

	for _, s := range ast.Statements {

		var res []interface{}
		var err error

		now := time.Now()

		switch stm := s.(type) {

		case *sql.UseStatement:
			continue

		case *sql.SelectStatement:
			res, err = executeSelectStatement(stm)
		case *sql.CreateStatement:
			res, err = executeCreateStatement(stm)
		case *sql.UpdateStatement:
			res, err = executeUpdateStatement(stm)
		case *sql.ModifyStatement:
			res, err = executeModifyStatement(stm)
		case *sql.DeleteStatement:
			res, err = executeDeleteStatement(stm)
		case *sql.RelateStatement:
			res, err = executeRelateStatement(stm)
		case *sql.RecordStatement:
			res, err = executeRecordStatement(stm)

		case *sql.DefineTableStatement:
			res, err = executeDefineTableStatement(stm)
		case *sql.RemoveTableStatement:
			res, err = executeRemoveTableStatement(stm)

		case *sql.DefineFieldStatement:
			res, err = executeDefineFieldStatement(stm)
		case *sql.RemoveFieldStatement:
			res, err = executeRemoveFieldStatement(stm)

		case *sql.DefineIndexStatement:
			res, err = executeDefineIndexStatement(stm)
		case *sql.ResyncIndexStatement:
			res, err = executeResyncIndexStatement(stm)
		case *sql.RemoveIndexStatement:
			res, err = executeRemoveIndexStatement(stm)

		}

		chn <- &Response{
			Time:   time.Since(now).String(),
			Status: status(err),
			Detail: detail(err),
			Result: append([]interface{}{}, res...),
		}

	}

	close(chn)

}
