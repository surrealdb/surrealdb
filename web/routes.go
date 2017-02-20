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

package web

import (
	"strconv"
	"time"

	"github.com/abcum/fibre"
	"github.com/abcum/fibre/mw"
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/sql"
)

func output(c *fibre.Context, err error, res []*db.Response) error {

	if err != nil {
		return fibre.NewHTTPError(500)
	}

	if len(res) == 0 {
		return fibre.NewHTTPError(500)
	}

	switch ret := res[0]; ret.Status {
	case "OK":
		return c.Send(200, ret.Result)
	case "ERR_DB":
		return fibre.NewHTTPError(503)
	case "ERR_TX":
		return fibre.NewHTTPError(500)
	case "ERR_KV":
		return fibre.NewHTTPError(409)
	case "ERR_CK":
		return fibre.NewHTTPError(403)
	default:
		return fibre.NewHTTPError(400)
	}

}

func limit(c *fibre.Context, i int64) int64 {
	if s := c.Query("limit"); len(s) > 0 {
		if x, err := strconv.ParseInt(s, 10, 64); err == nil {
			return x
		}
	}
	return i
}

func start(c *fibre.Context, i int64) int64 {
	if s := c.Query("start"); len(s) > 0 {
		if x, err := strconv.ParseInt(s, 10, 64); err == nil {
			return x
		}
	}
	return i
}

func trace(c *fibre.Context, i time.Time) time.Time {
	if s := c.Query("version"); len(s) > 0 {
		if x, err := time.Parse(sql.RFCNano, s); err == nil {
			return x
		}
	}
	return i
}

func routes(s *fibre.Fibre) {

	s.Dir("/", "app/")

	// --------------------------------------------------
	// Endpoint for health checks
	// --------------------------------------------------

	s.Get("/info", func(c *fibre.Context) error {
		return c.Code(200)
	})

	// --------------------------------------------------
	// Endpoints for submitting rpc queries
	// --------------------------------------------------

	s.Rpc("/rpc", &rpc{})

	// --------------------------------------------------
	// Endpoints for authentication signup
	// --------------------------------------------------

	s.Options("/signup", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Post("/signup", func(c *fibre.Context) error {
		return signup(c)
	})

	s.Use(mw.Type(&mw.TypeOpts{
		AllowedContent: map[string]bool{
			"application/json":                  true,
			"application/cbor":                  true,
			"application/msgpack":               true,
			"application/x-www-form-urlencoded": true,
		},
	}).PathIs("/signup"))

	// --------------------------------------------------
	// Endpoints for authentication signin
	// --------------------------------------------------

	s.Options("/signin", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Post("/signin", func(c *fibre.Context) error {
		return signin(c)
	})

	s.Use(mw.Type(&mw.TypeOpts{
		AllowedContent: map[string]bool{
			"application/json":                  true,
			"application/cbor":                  true,
			"application/msgpack":               true,
			"application/x-www-form-urlencoded": true,
		},
	}).PathIs("/signin"))

	// --------------------------------------------------
	// Endpoints for import and exporting data
	// --------------------------------------------------

	s.Get("/export", func(c *fibre.Context) error {
		return exporter(c)
	})

	s.Post("/import", func(c *fibre.Context) error {
		return importer(c)
	})

	s.Use(mw.Type(&mw.TypeOpts{
		AllowedContent: map[string]bool{
			"application/octet-stream": true,
		},
	}).PathIs("/export", "/import"))

	// --------------------------------------------------
	// Endpoints for submitting sql queries
	// --------------------------------------------------

	s.Options("/sql", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Post("/sql", func(c *fibre.Context) error {
		res, err := db.Execute(c, c.Request().Body, nil)
		if err != nil {
			return err
		}
		return c.Send(200, res)
	})

	s.Use(mw.Type(&mw.TypeOpts{
		AllowedContent: map[string]bool{
			"application/json":    true,
			"application/cbor":    true,
			"application/msgpack": true,
		},
	}).PathIs("/sql"))

	// --------------------------------------------------
	// Endpoints for submitting websocket sql
	// --------------------------------------------------

	s.Get("/sql", func(c *fibre.Context) error {
		if err := c.Upgrade(); err != nil {
			return err
		}
		for {
			_, msg, err := c.Socket().Read()
			if err != nil {
				return err
			}
			if res, err := db.Execute(c, msg, nil); err != nil {
				c.Socket().SendText(err.Error())
			} else {
				c.Socket().SendJSON(res)
			}
		}
		return nil
	})

	// --------------------------------------------------
	// Endpoints for manipulating multiple records
	// --------------------------------------------------

	s.Options("/key/:class", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Get("/key/:class", func(c *fibre.Context) error {

		txt := "SELECT * FROM $class LIMIT $limit START $start"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"class": sql.NewTable(c.Param("class")),
			"limit": limit(c, 100),
			"start": start(c, 0),
		})

		return output(c, err, res)

	})

	s.Post("/key/:class", func(c *fibre.Context) error {

		var data interface{}

		if err := c.Bind(&data); err != nil {
			return fibre.NewHTTPError(422)
		}

		txt := "CREATE $class CONTENT $data RETURN AFTER"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"class": sql.NewTable(c.Param("class")),
			"data":  data,
		})

		return output(c, err, res)

	})

	s.Delete("/key/:class", func(c *fibre.Context) error {

		txt := "DELETE FROM $class"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"class": sql.NewTable(c.Param("class")),
		})

		return output(c, err, res)

	})

	// --------------------------------------------------
	// Endpoints for manipulating a single record
	// --------------------------------------------------

	s.Options("/key/:class/:id", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Get("/key/:class/:id", func(c *fibre.Context) error {

		txt := "SELECT * FROM $thing VERSION $trace"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"thing": sql.NewThing(c.Param("class"), c.Param("id")),
			"trace": trace(c, time.Now()),
		})

		return output(c, err, res)

	})

	s.Post("/key/:class/:id", func(c *fibre.Context) error {

		var data interface{}

		if err := c.Bind(&data); err != nil {
			return fibre.NewHTTPError(422)
		}

		txt := "CREATE $thing CONTENT $data RETURN AFTER"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"thing": sql.NewThing(c.Param("class"), c.Param("id")),
			"data":  data,
		})

		return output(c, err, res)

	})

	s.Put("/key/:class/:id", func(c *fibre.Context) error {

		var data interface{}

		if err := c.Bind(&data); err != nil {
			return fibre.NewHTTPError(422)
		}

		txt := "UPDATE $thing CONTENT $data RETURN AFTER"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"thing": sql.NewThing(c.Param("class"), c.Param("id")),
			"data":  data,
		})

		return output(c, err, res)

	})

	s.Patch("/key/:class/:id", func(c *fibre.Context) error {

		var data interface{}

		if err := c.Bind(&data); err != nil {
			return fibre.NewHTTPError(422)
		}

		txt := "UPDATE $thing DIFF $data RETURN DIFF"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"thing": sql.NewThing(c.Param("class"), c.Param("id")),
			"data":  data,
		})

		return output(c, err, res)

	})

	s.Trace("/key/:class/:id", func(c *fibre.Context) error {

		txt := "SELECT HISTORY FROM $thing"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"thing": sql.NewThing(c.Param("class"), c.Param("id")),
		})

		return output(c, err, res)

	})

	s.Delete("/key/:class/:id", func(c *fibre.Context) error {

		txt := "DELETE $thing"

		res, err := db.Execute(c, txt, map[string]interface{}{
			"thing": sql.NewThing(c.Param("class"), c.Param("id")),
		})

		return output(c, err, res)

	})

	s.Use(mw.Type(&mw.TypeOpts{
		AllowedContent: map[string]bool{
			"application/json":         true,
			"application/cbor":         true,
			"application/msgpack":      true,
			"application/vnd.api+json": true,
		},
	}).PathBegsWith("/key/"))

}
