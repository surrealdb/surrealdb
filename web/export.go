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

package web

import (
	"github.com/surrealdb/fibre"
	"github.com/surrealdb/surrealdb/cnf"
	"github.com/surrealdb/surrealdb/db"
)

func export(c *fibre.Context) (err error) {

	if c.Get("auth").(*cnf.Auth).Kind >= cnf.AuthSC {
		return fibre.NewHTTPError(401)
	}

	c.Response().Header().Set("Content-Type", "application/octet-stream")

	return db.Export(c, c.Request().Header().Get("NS"), c.Request().Header().Get("DB"))

}
