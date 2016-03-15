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
	"encoding/base64"
	"strings"

	"github.com/labstack/echo"
)

// AuthOpts defines options for the Head middleward
type AuthOpts struct {
}

// Auth defines middleware for reading JWT authentication tokens
func Auth(opts *AuthOpts) echo.MiddlewareFunc {
	return func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {

			// TODO need to decide how users select the namespace and database
			c.Set("NS", "abcum")
			c.Set("DB", "onlineplatforms")

			head := c.Request().Header.Get("Authorization")

			auth := c.Get("opts.auth").(string)
			user := c.Get("opts.user").(string)
			pass := c.Get("opts.pass").(string)

			if auth == "" {
				return h(c)
			}

			if head != "" && head[:5] == "Basic" {

				base, _ := base64.StdEncoding.DecodeString(head[6:])

				cred := strings.SplitN(string(base), ":", 2)

				if len(cred) == 2 && cred[0] == user && cred[1] == pass {
					return h(c)
				}

			}

			return echo.NewHTTPError(401)

		}
	}
}
