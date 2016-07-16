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
	"github.com/abcum/fibre"
	"github.com/abcum/surreal/db"
)

func output(c *fibre.Context, res interface{}) error {
	switch ret := res.(*db.Response); ret.Status {
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

func routes(s *fibre.Fibre) {

	s.Dir("/", "app/")

	// --------------------------------------------------
	// Endpoint for health checks
	// --------------------------------------------------

	s.Get("/info", func(c *fibre.Context) error {
		return c.Code(200)
	})

	// --------------------------------------------------
	// Endpoint for database backups
	// --------------------------------------------------

	s.Get("/import", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Get("/export", func(c *fibre.Context) error {
		return c.Code(200)
	})

	// --------------------------------------------------
	// Endpoints for submitting rpc queries
	// --------------------------------------------------

	s.Rpc("/rpc", &rpc{})

	// --------------------------------------------------
	// Endpoints for submitting sql queries
	// --------------------------------------------------

	s.Options("/sql", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Post("/sql", func(c *fibre.Context) error {
		res, err := db.Execute(c, c.Request().Body)
		if err != nil {
			return c.Send(500, map[string]interface{}{
				"code":          500,
				"details":       "Request problems detected",
				"documentation": "https://docs.surreal.io/",
				"information":   err.Error(),
			})
		}
		return c.Send(200, res)
	})

	// --------------------------------------------------
	// Endpoints for manipulating multiple records
	// --------------------------------------------------

	s.Options("/key/:class", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Get("/key/:class", func(c *fibre.Context) error {
		sql := db.Prepare("SELECT * FROM ⟨%v⟩", c.Param("class"))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	s.Post("/key/:class", func(c *fibre.Context) error {
		sql := db.Prepare("CREATE ⟨%v⟩ CONTENT %v RETURN AFTER", c.Param("class"), string(c.Body()))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	s.Delete("/key/:class", func(c *fibre.Context) error {
		sql := db.Prepare("DELETE FROM ⟨%v⟩", c.Param("class"))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	// --------------------------------------------------
	// Endpoints for manipulating a single record
	// --------------------------------------------------

	s.Options("/key/:class/:id", func(c *fibre.Context) error {
		return c.Code(200)
	})

	s.Get("/key/:class/:id", func(c *fibre.Context) error {
		sql := db.Prepare("SELECT * FROM @⟨%v⟩:⟨%v⟩", c.Param("class"), c.Param("id"))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	s.Put("/key/:class/:id", func(c *fibre.Context) error {
		sql := db.Prepare("CREATE @⟨%v⟩:⟨%v⟩ CONTENT %v RETURN AFTER", c.Param("class"), c.Param("id"), string(c.Body()))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	s.Post("/key/:class/:id", func(c *fibre.Context) error {
		sql := db.Prepare("UPDATE @⟨%v⟩:⟨%v⟩ CONTENT %v RETURN AFTER", c.Param("class"), c.Param("id"), string(c.Body()))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	s.Patch("/key/:class/:id", func(c *fibre.Context) error {
		sql := db.Prepare("MODIFY @⟨%v⟩:⟨%v⟩ DIFF %v RETURN AFTER", c.Param("class"), c.Param("id"), string(c.Body()))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	s.Trace("/key/:class/:id", func(c *fibre.Context) error {
		sql := db.Prepare("SELECT HISTORY FROM @⟨%v⟩:⟨%v⟩", c.Param("class"), c.Param("id"))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

	s.Delete("/key/:class/:id", func(c *fibre.Context) error {
		sql := db.Prepare("DELETE @⟨%v⟩:⟨%v⟩", c.Param("class"), c.Param("id"))
		res, err := db.Execute(c, sql)
		if err != nil {
			return fibre.NewHTTPError(500)
		}
		return output(c, res[0])
	})

}
