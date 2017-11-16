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

package show

import (
	"github.com/abcum/fibre"
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/lang"
)

type Display int8

const (
	One Display = iota
	Many
)

type Method int8

const (
	Select Method = iota
	Create
	Update
	Delete
	Modify
	Trace
)

func Output(c *fibre.Context, t string, d Display, m Method, res []*db.Response, err error) error {

	switch err.(type) {
	case *sql.ParseError:
		return fibre.NewHTTPError(400) // Will happen if invalid json
	case *sql.BlankError:
		return fibre.NewHTTPError(403) // Will happen if invalid auth
	case *sql.QueryError:
		return fibre.NewHTTPError(403) // Should not happen
	case *sql.PermsError:
		return fibre.NewHTTPError(403) // Should not happen
	case *sql.EmptyError:
		return fibre.NewHTTPError(500) // Should not happen
	case *fibre.HTTPError:
		return err // Probably a timeout error
	}

	if len(res) == 0 {
		return fibre.NewHTTPError(500) // Should not happen
	}

	switch c.Type() {
	case "application/json":
		return OutputRest(c, t, d, m, res, err)
	case "application/cbor":
		return OutputRest(c, t, d, m, res, err)
	case "application/msgpack":
		return OutputRest(c, t, d, m, res, err)
	case "application/vnd.api+json":
		return OutputJson(c, t, d, m, res, err)
	}

	return nil

}

// --------------------------------------------------
// Endpoints for manipulating multiple records
// --------------------------------------------------

// OutputBase outputs the response data directly from the SQL
// query reponse without any manipulation or alteration.
func OutputBase(c *fibre.Context, t string, d Display, m Method, res []*db.Response, err error) error {

	var ret *db.Response

	switch ret = res[0]; ret.Status {
	case "OK":
		return c.Send(200, ret)
	case "ERR_DB":
		return fibre.NewHTTPError(503)
	case "ERR_KV":
		return fibre.NewHTTPError(409, ret.Detail)
	case "ERR_PE":
		return fibre.NewHTTPError(403, ret.Detail)
	case "ERR_FD":
		return fibre.NewHTTPError(422, ret.Detail)
	case "ERR_IX":
		return fibre.NewHTTPError(422, ret.Detail)
	default:
		return fibre.NewHTTPError(400, ret.Detail)
	}

}

// OutputRest outputs the json response data according to the specification
// available at http://emberjs.com/api/data/classes/DS.RESTAdapter.html and
// according to http://stackoverflow.com/questions/14922623.
func OutputRest(c *fibre.Context, t string, d Display, m Method, res []*db.Response, err error) error {

	var ret *db.Response

	switch ret = res[0]; ret.Status {
	case "OK":
		break
	case "ERR_DB":
		return fibre.NewHTTPError(503)
	case "ERR_KV":
		return fibre.NewHTTPError(409, ret.Detail)
	case "ERR_PE":
		return fibre.NewHTTPError(403, ret.Detail)
	case "ERR_FD":
		return fibre.NewHTTPError(422, ret.Detail)
	case "ERR_IX":
		return fibre.NewHTTPError(422, ret.Detail)
	default:
		return fibre.NewHTTPError(400, ret.Detail)
	}

	if len(res[0].Result) == 0 {
		return c.Send(204, nil)
	}

	switch m {
	case Delete:
		return c.Send(204, nil)
	case Create:
		return c.Send(201, map[string]interface{}{
			lang.Singularize(t): cleanRestOne(res[0].Result[0]),
		})
	}

	switch d {
	case One:
		return c.Send(200, map[string]interface{}{
			lang.Singularize(t): cleanRestOne(res[0].Result[0]),
		})
	case Many:
		return c.Send(200, map[string]interface{}{
			lang.Pluralize(t): cleanRestAll(res[0].Result),
		})
	}

	return c.Send(200, nil)

}

func cleanRestAll(vals []interface{}) (all []*data.Doc) {

	for _, val := range vals {
		all = append(all, cleanRestOne(val))
	}

	return

}

func cleanRestOne(val interface{}) (one *data.Doc) {

	one = data.Consume(val)
	one.Iff(one.Get("meta.id").Data(), "id")

	return

}

// OutputJson outputs the json response data according to the specification
// available at http://jsonapi.org/format/. Currently linked data does not
// adhere to the specification, but is intead displayed inside the attribute
// object. Links and embedded paths are also not implemented.
func OutputJson(c *fibre.Context, t string, d Display, m Method, res []*db.Response, err error) error {

	var ret *db.Response

	switch ret = res[0]; ret.Status {
	case "OK":
		break
	case "ERR_DB":
		return fibre.NewHTTPError(503)
	case "ERR_KV":
		return fibre.NewHTTPError(409, ret.Detail)
	case "ERR_PE":
		return fibre.NewHTTPError(403, ret.Detail)
	case "ERR_FD":
		return fibre.NewHTTPError(422, ret.Detail)
	case "ERR_IX":
		return fibre.NewHTTPError(422, ret.Detail)
	default:
		return fibre.NewHTTPError(400, ret.Detail)
	}

	if len(res[0].Result) == 0 {
		return c.Send(204, nil)
	}

	switch m {
	case Delete:
		return c.Send(204, nil)
	case Create:
		return c.Send(201, map[string]interface{}{
			"data": cleanJsonOne(res[0].Result[0]),
		})
	}

	switch d {
	case One:
		return c.Send(200, map[string]interface{}{
			"data": cleanJsonOne(res[0].Result[0]),
		})
	case Many:
		return c.Send(200, map[string]interface{}{
			"data": cleanJsonAll(res[0].Result),
		})
	}

	return c.Send(200, nil)

}

func cleanJsonAll(vals []interface{}) (all []*data.Doc) {

	for _, val := range vals {
		all = append(all, cleanJsonOne(val))
	}

	return

}

func cleanJsonOne(val interface{}) (one *data.Doc) {

	one = data.New()
	old := data.Consume(val)
	one.Set(old.Get("meta.id").Data(), "id")
	one.Set(old.Get("meta.tb").Data(), "type")
	one.Set(old.Data(), "attributes")
	one.Del("attributes.id")
	one.Del("attributes.meta")

	return

}
