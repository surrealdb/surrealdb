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
	"testing"
	"time"

	"net/http"
	"net/http/httptest"

	. "github.com/smartystreets/goconvey/convey"
)

type Handler struct {
	sync.Mutex
	count int
}

func (s *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Lock()
	defer s.Unlock()
	s.count++
	fmt.Fprintf(w, "SERVER OK: %d", s.count)
}

func TestRun(t *testing.T) {

	handler := &Handler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	Convey("Run statement which runs http requests", t, func() {

		setupDB(1)

		func() {

			txt := `
			USE NS test DB test;
			DEFINE TABLE test PERMISSIONS FULL;
			DEFINE TABLE temp PERMISSIONS FOR SELECT FULL;
			DEFINE EVENT done ON test WHEN true THEN (
				CREATE temp:main;
				RUN http.get("` + server.URL + `");
				RUN http.put("` + server.URL + `");
				RUN http.post("` + server.URL + `");
				RUN http.delete("` + server.URL + `");
				RUN http.async.get("` + server.URL + `");
				RUN http.async.put("` + server.URL + `");
				RUN http.async.post("` + server.URL + `");
				RUN http.async.delete("` + server.URL + `");
			);
			`

			res, err := Execute(permsKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 4)
			So(res[1].Status, ShouldEqual, "OK")
			So(res[2].Status, ShouldEqual, "OK")

		}()

		func() {

			txt := `
			USE NS test DB test;
			SELECT * FROM test, temp;
			CREATE test:main;
			SELECT * FROM test, temp;
			`

			res, err := Execute(permsSC(), txt, nil)
			time.Sleep(1 * time.Second)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 4)
			So(res[1].Status, ShouldEqual, "OK")
			So(res[1].Result, ShouldHaveLength, 0)
			So(res[2].Status, ShouldEqual, "OK")
			So(res[2].Result, ShouldHaveLength, 1)
			So(res[3].Status, ShouldEqual, "OK")
			So(res[3].Result, ShouldHaveLength, 2)

			handler.Lock()
			So(handler.count, ShouldEqual, 8)
			handler.Unlock()

		}()

	})

}
