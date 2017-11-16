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
	"sync"

	"context"

	"github.com/abcum/fibre"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/uuid"
)

type socket struct {
	mutex sync.Mutex
	fibre *fibre.Context
	waits []interface{}
	lives map[string]*sql.LiveStatement
}

func clear() {
	for _, s := range sockets {
		s.clear()
	}
}

func flush() {
	for _, s := range sockets {
		s.flush()
	}
}

func (s *socket) ctx() (ctx context.Context) {

	ctx = context.Background()

	auth := s.fibre.Get(varKeyAuth).(*cnf.Auth)
	ctx = context.WithValue(ctx, ctxKeyAuth, auth.Data)
	ctx = context.WithValue(ctx, ctxKeyKind, auth.Kind)

	vars := data.New()
	vars.Set(auth.Data, varKeyAuth)
	vars.Set(auth.Scope, varKeyScope)
	vars.Set(s.fibre.Origin(), varKeyOrigin)
	vars.Set(s.fibre.IP().String(), varKeyIp)
	ctx = context.WithValue(ctx, ctxKeyVars, vars)

	return

}

func (s *socket) queue(query, action string, result interface{}) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.waits = append(s.waits, &Dispatch{
		Query:  query,
		Action: action,
		Result: result,
	})

}

func (s *socket) clear() (err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.waits = nil

	return

}

func (s *socket) flush() (err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If there are no pending message
	// notifications for this socket
	// then ignore this method call.

	if len(s.waits) == 0 {
		return nil
	}

	// Create a new rpc notification
	// object so that we can send the
	// batch changes in one go.

	obj := &fibre.RPCNotification{
		Method: "notify",
		Params: s.waits,
	}

	// Check the websocket subprotocol
	// and send the relevant message
	// type containing the notification.

	sock := s.fibre.Socket()

	switch sock.Subprotocol() {
	default:
		err = sock.SendJSON(obj)
	case "json":
		err = sock.SendJSON(obj)
	case "cbor":
		err = sock.SendCBOR(obj)
	case "pack":
		err = sock.SendPACK(obj)
	}

	// Make sure that we clear all the
	// pending message notifications
	// for this socket when done.

	s.waits = nil

	return

}

func (s *socket) check(e *executor, ctx context.Context, stm *sql.LiveStatement) (err error) {

	var tb *sql.DefineTableStatement

	// If we are authenticated using DB, NS,
	// or KV permissions level, then we can
	// ignore all permissions checks.

	if ctx.Value(ctxKeyKind).(cnf.Kind) < cnf.AuthSC {
		return nil
	}

	// First check that the NS exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, err = e.dbo.GetNS(stm.NS)
	if err != nil {
		return err
	}

	// Next check that the DB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, err = e.dbo.GetDB(stm.NS, stm.DB)
	if err != nil {
		return err
	}

	// Then check that the TB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	tb, err = e.dbo.GetTB(stm.NS, stm.DB, stm.What.TB)
	if err != nil {
		return err
	}

	// If the table does exist we then try
	// to process the relevant permissions
	// expression, but only if they don't
	// reference any document fields.

	var val interface{}

	switch p := tb.Perms.(type) {
	case *sql.PermExpression:
		val, err = e.fetch(ctx, p.Select, ign)
	default:
		return &PermsError{table: stm.What.TB}
	}

	// If we receive an 'ident failed' error
	// it is because the table permission
	// expression contains a field check,
	// and therefore we must check each
	// record individually to see if it can
	// be accessed or not.

	if err != queryIdentFailed {
		if val, ok := val.(bool); ok && !val {
			return &PermsError{table: stm.What.TB}
		}
	}

	return

}

func (s *socket) deregister(id string) {

	delete(sockets, id)

	txn, _ := db.Begin(true)

	defer txn.Commit()

	for id, stm := range s.lives {

		key := &keys.LV{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: stm.What.TB, LV: id}
		txn.Clr(key.Encode())

	}

}

func (s *socket) executeLive(e *executor, ctx context.Context, stm *sql.LiveStatement) (out []interface{}, err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check that we are allowed to perform
	// the live query on the specified table
	// and if we can't then return an error
	// and don't save the live query.

	err = s.check(e, ctx, stm)
	if err != nil {
		return nil, err
	}

	// Generate a new uuid for this query,
	// which we will use to identify the
	// query when sending push messages
	// and when killing the query.

	stm.ID = uuid.NewV4().String()

	// Store the live query on the socket.

	s.lives[stm.ID] = stm

	// Add the live query to the database
	// under the relevant NS, DB, and TB.

	key := &keys.LV{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: stm.What.TB, LV: stm.ID}
	_, err = e.dbo.Put(0, key.Encode(), stm.Encode())

	// Return the query id to the user.

	out = append(out, stm.ID)

	return

}

func (s *socket) executeKill(e *executor, ctx context.Context, stm *sql.KillStatement) (out []interface{}, err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Get the specified query on this socket.

	if qry, ok := s.lives[stm.Name.ID]; ok {

		// Delete the live query from the saved queries.

		delete(s.lives, qry.ID)

		// Delete the live query from the database layer.

		key := &keys.LV{KV: qry.KV, NS: qry.NS, DB: qry.DB, TB: qry.What.TB, LV: qry.ID}
		_, err = e.dbo.Clr(key.Encode())

	}

	return

}
