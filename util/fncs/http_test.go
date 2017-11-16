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

package fncs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestHttp(t *testing.T) {

	var res interface{}
	var jsn = `{"test":true, "temp":"text"}`
	var obj = map[string]interface{}{"temp": "text", "test": true}
	var txt = `<!doctype html><html><head></head><body></body></html>`
	var opt = map[string]interface{}{
		"auth": map[string]interface{}{"user": "u", "pass": "p"},
		"head": map[string]interface{}{"x-test": true},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "HEAD", "DELETE":
			// Return nothing
		case "GET":
			fmt.Fprint(w, txt)
		case "PUT", "POST", "PATCH":
			io.Copy(w, r.Body)
		}
	}))

	defer srv.Close()

	Convey("http.head(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.head", srv.URL)
		So(res, ShouldResemble, nil)
	})

	Convey("http.get(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.get", srv.URL)
		So(res, ShouldResemble, []byte(txt))
		res, _ = Run(context.Background(), "http.get", srv.URL, opt)
		So(res, ShouldResemble, []byte(txt))
	})

	Convey("http.put(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.put", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.put", srv.URL, txt)
		So(res, ShouldResemble, []byte(txt))
		res, _ = Run(context.Background(), "http.put", srv.URL, jsn)
		So(res, ShouldResemble, obj)
		res, _ = Run(context.Background(), "http.put", srv.URL, jsn, opt)
		So(res, ShouldResemble, obj)
	})

	Convey("http.post(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.post", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.post", srv.URL, txt)
		So(res, ShouldResemble, []byte(txt))
		res, _ = Run(context.Background(), "http.post", srv.URL, jsn)
		So(res, ShouldResemble, obj)
		res, _ = Run(context.Background(), "http.post", srv.URL, jsn, opt)
		So(res, ShouldResemble, obj)
	})

	Convey("http.patch(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.patch", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.patch", srv.URL, txt)
		So(res, ShouldResemble, []byte(txt))
		res, _ = Run(context.Background(), "http.patch", srv.URL, jsn)
		So(res, ShouldResemble, obj)
		res, _ = Run(context.Background(), "http.patch", srv.URL, jsn, opt)
		So(res, ShouldResemble, obj)
	})

	Convey("http.delete(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.delete", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.delete", srv.URL, opt)
		So(res, ShouldResemble, nil)
	})

	Convey("http.async.head(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.async.head", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.head", srv.URL, opt)
		So(res, ShouldResemble, nil)
	})

	Convey("http.async.get(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.async.get", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.get", srv.URL, opt)
		So(res, ShouldResemble, nil)
	})

	Convey("http.async.put(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.async.put", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.put", srv.URL, txt)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.put", srv.URL, jsn)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.put", srv.URL, jsn, opt)
		So(res, ShouldResemble, nil)
	})

	Convey("http.async.post(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.async.post", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.post", srv.URL, txt)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.post", srv.URL, jsn)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.post", srv.URL, jsn, opt)
		So(res, ShouldResemble, nil)
	})

	Convey("http.async.patch(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.async.patch", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.patch", srv.URL, txt)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.patch", srv.URL, jsn)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.patch", srv.URL, jsn, opt)
		So(res, ShouldResemble, nil)
	})

	Convey("http.async.delete(a) works properly", t, func() {
		res, _ = Run(context.Background(), "http.async.delete", srv.URL)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "http.async.delete", srv.URL, opt)
		So(res, ShouldResemble, nil)
	})

}
