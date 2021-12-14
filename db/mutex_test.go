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
	"context"
	"sync"
	"testing"

	"github.com/surrealdb/surrealdb/util/data"
	. "github.com/smartystreets/goconvey/convey"
)

type stringer struct{}

func (this stringer) String() string {
	return "test"
}

func TestMutex(t *testing.T) {

	var n = 10

	Convey("Context diving works correctly", t, func() {

		ctx := context.Background()

		So(vers(ctx), ShouldEqual, 0)

		for i := vers(ctx); i <= maxRecursiveQueries; i++ {
			So(func() { ctx = dive(ctx) }, ShouldNotPanic)
			So(vers(ctx), ShouldEqual, i+1)
		}

		So(func() { dive(ctx) }, ShouldPanicWith, errRecursiveOverload)

	})

	Convey("Allow basic mutex", t, func() {

		m := new(mutex)
		ctx := context.Background()

		m.Lock(ctx, new(stringer))
		m.Unlock(ctx, new(stringer))

	})

	Convey("Allow concurrent mutex", t, func() {

		m := new(mutex)
		wg := new(sync.WaitGroup)
		ctx := context.Background()

		wg.Add(n)

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				m.Lock(ctx, new(stringer))
				m.Unlock(ctx, new(stringer))
			}()
		}

		wg.Wait()

		So(nil, ShouldBeNil)

	})

	Convey("Allow fixed-level mutex", t, func() {

		m := new(mutex)
		ctx := context.Background()

		for i := 0; i < n; i++ {
			ctx = dive(ctx)
			So(func() { m.Lock(ctx, new(stringer)) }, ShouldNotPanic)
			So(func() { m.Unlock(ctx, new(stringer)) }, ShouldNotPanic)
		}

		So(nil, ShouldBeNil)

	})

	Convey("Prevent nested-recursive mutex", t, func() {

		m := new(mutex)
		ctx := context.Background()

		m.Lock(ctx, new(stringer))
		ctx = dive(ctx)
		So(func() { m.Lock(ctx, new(stringer)) }, ShouldPanic)
		So(func() { m.Unlock(ctx, new(stringer)) }, ShouldNotPanic)
		So(func() { m.Unlock(ctx, new(stringer)) }, ShouldNotPanic)

		So(nil, ShouldBeNil)

	})

	Convey("Ensure document locking when multiple events attempt to write to the same document", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		DEFINE EVENT created ON person WHEN $method = "CREATE" THEN (UPDATE $after.fk SET fks += $this);
		DEFINE EVENT deleted ON person WHEN $method = "DELETE" THEN (UPDATE $before.fk SET fks -= $this);
		UPDATE |person:1..100| SET fk = other:test;
		SELECT * FROM other;
		DELETE FROM person;
		SELECT * FROM other;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 100)
		So(res[4].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("fks").Data(), ShouldHaveLength, 100)
		So(res[5].Status, ShouldEqual, "OK")
		So(res[5].Result, ShouldHaveLength, 0)
		So(res[6].Status, ShouldEqual, "OK")
		So(res[6].Result, ShouldHaveLength, 1)
		So(data.Consume(res[6].Result[0]).Get("fks").Data(), ShouldHaveLength, 0)

	})

	Convey("Ability to select the same document in a SELECT subquery", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE person:test;
		SELECT * FROM (SELECT * FROM (SELECT * FROM person));
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)

	})

	Convey("Ability to update the same document in a SELECT subquery", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE person:test;
		SELECT * FROM (UPDATE person SET test=true);
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "OK")
		So(data.Consume(res[2].Result[0]).Get("temp").Data(), ShouldBeNil)
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("temp").Data(), ShouldBeNil)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, true)

	})

	Convey("Ability to update the same document in a SELECT subquery", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE person:test;
		SELECT *, (UPDATE $parent.id SET test=true) AS test FROM person;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "OK")
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("temp").Data(), ShouldBeNil)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, true)

	})

	Convey("Inability to update the same document in an UPDATE subquery", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE person:test;
		UPDATE person SET temp = (UPDATE person SET test=true);
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "ERR")
		So(res[2].Detail, ShouldEqual, "Failed to update the same document recursively")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldBeNil)
		So(data.Consume(res[3].Result[0]).Get("temp").Data(), ShouldBeNil)

	})

	Convey("Ability to update the same document in an event", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $before.test != $after.test THEN (UPDATE $this SET temp = true);
		UPDATE person:test SET test=true;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(data.Consume(res[2].Result[0]).Get("temp").Data(), ShouldBeNil)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(res[3].Status, ShouldEqual, "OK")
		So(data.Consume(res[3].Result[0]).Get("temp").Data(), ShouldEqual, true)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, true)

	})

	Convey("Subqueries for an event should be on the same level", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $method = "CREATE" THEN (CREATE tester);
		CREATE |person:100|;
		SELECT * FROM person;
		SELECT * FROM tester;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[2].Result, ShouldHaveLength, 100)
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 100)
		So(res[4].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 100)

	})

	Convey("Subqueries for an event on a different level create an infinite loop", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $method = "CREATE" THEN (CREATE person);
		CREATE person:test;
		SELECT * FROM person;
		SELECT * FROM tester;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "ERR")
		So(res[2].Detail, ShouldEqual, "Infinite loop when running recursive subqueries")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[4].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 0)

	})

	Convey("Subqueries for recursive events on a different level create an infinite loop", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $method = "UPDATE" THEN (UPDATE tester SET temp=time.now());
		DEFINE EVENT test ON tester WHEN $method = "UPDATE" THEN (UPDATE person SET temp=time.now());
		CREATE person:test, tester:test SET temp=time.now();
		UPDATE person:test SET temp=time.now();
		SELECT * FROM person;
		SELECT * FROM tester;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[4].Status, ShouldEqual, "ERR")
		So(res[4].Detail, ShouldEqual, "Infinite loop when running recursive subqueries")
		So(res[5].Status, ShouldEqual, "OK")
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Status, ShouldEqual, "OK")
		So(res[6].Result, ShouldHaveLength, 1)

	})

	Convey("Ability to define complex dependent events which should run consecutively and succeed", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE global:test SET tests=[], temps=[];
		DEFINE EVENT test ON tester WHEN $after.global != EMPTY THEN (
			UPDATE $after.global SET tests+=$this;
			UPDATE temper SET tester=$this, global=$after.global;
		);
		DEFINE EVENT test ON temper WHEN $after.global != EMPTY THEN (
			UPDATE $after.global SET temps+=$this;
		);
		CREATE |temper:1..5|;
		CREATE tester:test SET global=global:test;
		SELECT * FROM global;
		SELECT * FROM tester;
		SELECT * FROM temper;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 9)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[4].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 5)
		So(res[5].Status, ShouldEqual, "OK")
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Status, ShouldEqual, "OK")
		So(res[6].Result, ShouldHaveLength, 1)
		So(res[7].Status, ShouldEqual, "OK")
		So(res[7].Result, ShouldHaveLength, 1)
		So(res[8].Status, ShouldEqual, "OK")
		So(res[8].Result, ShouldHaveLength, 5)

	})

}
