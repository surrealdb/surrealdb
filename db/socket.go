// Copyright © 2016 SurrealDB Ltd.
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

	"github.com/surrealdb/fibre"

	"github.com/surrealdb/surrealdb/cnf"
	"github.com/surrealdb/surrealdb/kvs"
	"github.com/surrealdb/surrealdb/sql"
	"github.com/surrealdb/surrealdb/txn"
	"github.com/surrealdb/surrealdb/util/data"
	"github.com/surrealdb/surrealdb/util/keys"
	"github.com/surrealdb/surrealdb/util/uuid"
)

type socket struct {
	mutex sync.Mutex
	fibre *fibre.Context
	sends map[string][]interface{}
	items map[string][]interface{}
	lives map[string]*sql.LiveStatement
}

func clear(id string) {
	go func() {
		sockets.Range(func(key, val interface{}) bool {
			val.(*socket).clear(id + "-bg")
			val.(*socket).clear(id)
			return true
		})
	}()
}

func flush(id string) {
	go func() {
		sockets.Range(func(key, val interface{}) bool {
			val.(*socket).flush(id + "-bg")
			val.(*socket).flush(id)
			return true
		})
	}()
}

func send(id string) {
	go func() {
		sockets.Range(func(key, val interface{}) bool {
			val.(*socket).send(id + "-bg")
			val.(*socket).send(id)
			return true
		})
	}()
}

// TODO remove this when distributed
// We need to remove this when moving
// to a distributed cluster as
// websockets might be managed by an
// alternative server, and should not
// be removed on node startup.

func tidy() error {

	ctx := context.Background()

	txn, _ := txn.New(ctx, true)

	defer txn.Commit()

	nss, err := txn.AllNS(ctx)
	if err != nil {
		return err
	}

	for _, ns := range nss {

		dbs, err := txn.AllDB(ctx, ns.Name.VA)
		if err != nil {
			return err
		}

		for _, db := range dbs {

			tbs, err := txn.AllTB(ctx, ns.Name.VA, db.Name.VA)
			if err != nil {
				return err
			}

			for _, tb := range tbs {

				key := &keys.LV{KV: KV, NS: ns.Name.VA, DB: db.Name.VA, TB: tb.Name.VA, LV: keys.Ignore}
				if _, err = txn.ClrP(ctx, key.Encode(), 0); err != nil {
					return err
				}

			}

		}

	}

	return nil

}

func (s *socket) ctx() (ctx context.Context) {

	ctx = context.Background()

	auth := s.fibre.Get(ctxKeyAuth).(*cnf.Auth)
	sess := s.fibre.Get(ctxKeyVars).(map[string]interface{})

	vars := data.Consume(sess)
	vars.Set(ENV, varKeyEnv)
	vars.Set(auth.Data, varKeyAuth)
	vars.Set(auth.Scope, varKeyScope)
	vars.Set(session(s.fibre), varKeySession)
	ctx = context.WithValue(ctx, ctxKeyVars, vars)
	ctx = context.WithValue(ctx, ctxKeyKind, auth.Kind)

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

	delete(s.items, id)

	return

}

func (s *socket) flush(id string) (err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.sends[id] = append(s.sends[id], s.items[id]...)

	delete(s.items, id)

	return

}

func (s *socket) send(id string) (err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If there are no pending message
	// notifications for this socket
	// then ignore this method call.

	if len(s.sends[id]) == 0 {
		return nil
	}

	// Create a new rpc notification
	// object so that we can send the
	// batch changes in one go.

	obj := &fibre.RPCNotification{
		Method: "notify",
		Params: s.sends[id],
	}

	// Notify the websocket connection
	// y sending an RPCNotification type
	// to the notify channel.

	s.fibre.Socket().Notify(obj)

	// Make sure that we clear all the
	// pending message notifications
	// for this socket when done.

	delete(s.sends, id)

	return

}

func (s *socket) check(e *executor, ctx context.Context, ns, db, tb string) (err error) {

	var tbv *sql.DefineTableStatement

	// If we are authenticated using DB, NS,
	// or KV permissions level, then we can
	// ignore all permissions checks.

	if perm(ctx) < cnf.AuthSC {
		return nil
	}

	// First check that the NS exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, err = e.tx.GetNS(ctx, ns)
	if err != nil {
		return err
	}

	// Next check that the DB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, err = e.tx.GetDB(ctx, ns, db)
	if err != nil {
		return err
	}

	// Then check that the TB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	tbv, err = e.tx.GetTB(ctx, ns, db, tb)
	if err != nil {
		return err
	}

	// If the table has any permissions
	// specified, then let's check if this
	// query is allowed access to the table.

	switch p := tbv.Perms.(type) {
	case *sql.PermExpression:
		return e.fetchPerms(ctx, p.Select, tbv.Name)
	default:
		return &PermsError{table: tb}
	}

}

func (s *socket) deregister(id string) {

	sockets.Delete(id)

	ctx := context.Background()

	txn, _ := kvs.Begin(ctx, true)

	defer txn.Commit()

	for id, stm := range s.lives {

		for _, w := range stm.What {

			switch what := w.(type) {

			case *sql.Table:

				key := &keys.LV{KV: KV, NS: stm.NS, DB: stm.DB, TB: what.TB, LV: id}
				txn.Clr(ctx, key.Encode())

			case *sql.Ident:

				key := &keys.LV{KV: KV, NS: stm.NS, DB: stm.DB, TB: what.VA, LV: id}
				txn.Clr(ctx, key.Encode())

			}

		}

	}

}

func (s *socket) executeLive(e *executor, ctx context.Context, stm *sql.LiveStatement) (out []interface{}, err error) {

	stm.FB = e.id
	stm.NS = e.ns
	stm.DB = e.db

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

			key := &keys.LV{KV: KV, NS: stm.NS, DB: stm.DB, TB: what.TB, LV: stm.ID}
			if _, err = e.tx.Put(ctx, 0, key.Encode(), stm.Encode()); err != nil {
				return nil, err
			}

		case *sql.Ident:

			key := &keys.LV{KV: KV, NS: stm.NS, DB: stm.DB, TB: what.VA, LV: stm.ID}
			if _, err = e.tx.Put(ctx, 0, key.Encode(), stm.Encode()); err != nil {
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

	var what sql.Exprs

	for _, val := range stm.What {
		w, err := e.fetch(ctx, val, nil)
		if err != nil {
			return nil, err
		}
		what = append(what, w)
	}

	for _, w := range what {

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
						key := &keys.LV{KV: KV, NS: qry.NS, DB: qry.DB, TB: what.TB, LV: qry.ID}
						_, err = e.tx.Clr(ctx, key.Encode())

					case *sql.Ident:
						key := &keys.LV{KV: KV, NS: qry.NS, DB: qry.DB, TB: what.VA, LV: qry.ID}
						_, err = e.tx.Clr(ctx, key.Encode())

					}

				}

			}

		}

	}

	return

}
