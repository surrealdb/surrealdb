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
	items map[string][]interface{}
	lives map[string]*sql.LiveStatement
}

func clear(id string) {
	for _, s := range sockets {
		s.clear(id)
	}
}

func flush(id string) {
	for _, s := range sockets {
		s.flush(id)
	}
}

func (s *socket) ctx(ns, db string) (ctx context.Context) {

	ctx = context.Background()

	ctx = context.WithValue(ctx, ctxKeyNs, ns)
	ctx = context.WithValue(ctx, ctxKeyDb, db)

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

func (s *socket) queue(id, query, action string, result interface{}) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.items[id] = append(s.items[id], &Dispatch{
		Query:  query,
		Action: action,
		Result: result,
	})

}

func (s *socket) clear(id string) (err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.items[id] = nil

	return

}

func (s *socket) flush(id string) (err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If there are no pending message
	// notifications for this socket
	// then ignore this method call.

	if len(s.items[id]) == 0 {
		return nil
	}

	// Create a new rpc notification
	// object so that we can send the
	// batch changes in one go.

	obj := &fibre.RPCNotification{
		Method: "notify",
		Params: s.items[id],
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

	s.items[id] = nil

	return

}

func (s *socket) check(e *executor, ctx context.Context, ns, db, tb string) (err error) {

	var tbv *sql.DefineTableStatement

	// If we are authenticated using DB, NS,
	// or KV permissions level, then we can
	// ignore all permissions checks.

	if ctx.Value(ctxKeyKind).(cnf.Kind) < cnf.AuthSC {
		return nil
	}

	// First check that the NS exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, err = e.dbo.GetNS(ns)
	if err != nil {
		return err
	}

	// Next check that the DB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, err = e.dbo.GetDB(ns, db)
	if err != nil {
		return err
	}

	// Then check that the TB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	tbv, err = e.dbo.GetTB(ns, db, tb)
	if err != nil {
		return err
	}

	// Once we have the table we reset the
	// context to DB level so that no other
	// embedded permissions are checked on
	// records within these permissions.

	ctx = context.WithValue(ctx, ctxKeyKind, cnf.AuthDB)

	// If the table does exist we then try
	// to process the relevant permissions
	// expression, but only if they don't
	// reference any document fields.

	var val interface{}

	switch p := tbv.Perms.(type) {
	case *sql.PermExpression:
		val, err = e.fetch(ctx, p.Select, ign)
	default:
		return &PermsError{table: tb}
	}

	// If we receive an 'ident failed' error
	// it is because the table permission
	// expression contains a field check,
	// and therefore we must check each
	// record individually to see if it can
	// be accessed or not.

	if err != queryIdentFailed {
		if val, ok := val.(bool); ok && !val {
			return &PermsError{table: tb}
		}
	}

	return nil

}

func (s *socket) deregister(id string) {

	delete(sockets, id)

	txn, _ := db.Begin(context.Background(), true)

	defer txn.Commit()

	for id, stm := range s.lives {

		for _, w := range stm.What {

			switch what := w.(type) {

			case *sql.Table:

				key := &keys.LV{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.TB, LV: id}
				txn.Clr(key.Encode())

			case *sql.Ident:

				key := &keys.LV{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.ID, LV: id}
				txn.Clr(key.Encode())

			}

		}

	}

}

func (s *socket) executeLive(e *executor, ctx context.Context, stm *sql.LiveStatement) (out []interface{}, err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Generate a new query uuid.

	stm.ID = uuid.New().String()

	// Store the live query on the socket.

	s.lives[stm.ID] = stm

	// Return the query id to the user.

	out = append(out, stm.ID)

	// Store the live query in the database layer.

	for key, val := range stm.What {
		w, err := e.fetch(ctx, val, nil)
		if err != nil {
			return nil, err
		}
		stm.What[key] = w
	}

	for _, w := range stm.What {

		switch what := w.(type) {

		default:
			return nil, fmt.Errorf("Can not execute LIVE query using value '%v'", what)

		case *sql.Table:

			if err = s.check(e, ctx, stm.NS, stm.DB, what.TB); err != nil {
				return nil, err
			}

			key := &keys.LV{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.TB, LV: stm.ID}
			if _, err = e.dbo.Put(0, key.Encode(), stm.Encode()); err != nil {
				return nil, err
			}

		case *sql.Ident:

			if err = s.check(e, ctx, stm.NS, stm.DB, what.ID); err != nil {
				return nil, err
			}

			key := &keys.LV{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.ID, LV: stm.ID}
			if _, err = e.dbo.Put(0, key.Encode(), stm.Encode()); err != nil {
				return nil, err
			}

		}

	}

	return

}

func (s *socket) executeKill(e *executor, ctx context.Context, stm *sql.KillStatement) (out []interface{}, err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Remove the live query from the database layer.

	for key, val := range stm.What {
		w, err := e.fetch(ctx, val, nil)
		if err != nil {
			return nil, err
		}
		stm.What[key] = w
	}

	for _, w := range stm.What {

		switch what := w.(type) {

		default:
			return nil, fmt.Errorf("Can not execute KILL query using value '%v'", what)

		case string:

			if qry, ok := s.lives[what]; ok {

				// Delete the live query from the saved queries.

				delete(s.lives, qry.ID)

				// Delete the live query from the database layer.

				for _, w := range qry.What {

					switch what := w.(type) {

					case *sql.Table:
						key := &keys.LV{KV: qry.KV, NS: qry.NS, DB: qry.DB, TB: what.TB, LV: qry.ID}
						_, err = e.dbo.Clr(key.Encode())

					case *sql.Ident:
						key := &keys.LV{KV: qry.KV, NS: qry.NS, DB: qry.DB, TB: what.ID, LV: qry.ID}
						_, err = e.dbo.Clr(key.Encode())

					}

				}

			}

		}

	}

	return

}
