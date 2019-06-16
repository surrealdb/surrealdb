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
	"net/http/httptest"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/util/uuid"
)

var req *fibre.Request
var res *fibre.Response

func init() {
	req = &fibre.Request{Request: httptest.NewRequest("GET", "/", nil)}
	res = &fibre.Response{}
}

func setupDB(workers int) {

	cnf.Settings = &cnf.Options{}
	cnf.Settings.DB.Path = "memory"
	cnf.Settings.DB.Base = "surreal"
	workerCount = workers

	Setup(cnf.Settings)

}

func permsKV() (ctx *fibre.Context) {

	ctx = fibre.NewContext(req, res, nil)
	ctx.Set("id", uuid.New().String())
	ctx.Set("auth", &cnf.Auth{
		Kind: cnf.AuthKV,
		NS:   "test",
		DB:   "test",
	})

	return ctx

}

func permsSC() (ctx *fibre.Context) {

	ctx = fibre.NewContext(req, res, nil)
	ctx.Set("id", uuid.New().String())
	ctx.Set("auth", &cnf.Auth{
		Kind: cnf.AuthSC,
		NS:   "test",
		DB:   "test",
	})

	return ctx

}
