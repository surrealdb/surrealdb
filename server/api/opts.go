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

package api

import (
	"github.com/abcum/surreal/cnf"
	"github.com/labstack/echo"
)

// Opts defines middleware for storing Surreal server options in the context
func Opts(opts *cnf.Options) echo.MiddlewareFunc {
	return func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {

			c.Set("opts", opts)

			c.Set("opts.db", opts.Db)

			c.Set("opts.base", opts.Base)

			c.Set("opts.auth", opts.Auth)
			c.Set("opts.user", opts.User)
			c.Set("opts.pass", opts.Pass)

			c.Set("opts.port", opts.Port)
			c.Set("opts.http", opts.Http)
			c.Set("opts.sock", opts.Sock)

			return h(c)

		}
	}
}
