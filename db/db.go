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
	"github.com/abcum/surreal/err"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/stop"
)

type Response struct {
	Time   string      `json:"time, omitempty"`
	Status interface{} `json:"status,omitempty"`
	Detail interface{} `json:"detail,omitempty"`
	Result interface{} `json:"result,omitempty"`
}

var db *client.DB
var st *stop.Stopper

// Setup sets up the connection with the data layer
func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("db").Infof("Connecting to database at %s", opts.Store)

	st = stop.NewStopper()

	ct := rpc.NewContext(&base.Context{User: "node", Insecure: true}, nil, st)

	se, err := client.NewSender(ct, opts.Store)
	if err != nil {
		st.Stop()
		log.WithPrefix("db").Errorf("failed to initialize KV client: %s", err)
		return
	}

	db = client.NewDB(se)

	return

}

// Exit shuts down the connection with the data layer
func Exit() {

	log.WithPrefix("db").Infof("Disconnecting from database")

	st.Stop()

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
			Status: errors.Status(err),
			Detail: errors.Detail(err),
			Result: append([]interface{}{}, res...),
		}

	}

	close(chn)

}
