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
	"github.com/labstack/echo"
)

// InfoOpts defines options for the Head middleward
type InfoOpts struct {
	PoweredBy string
}

// Info defines middleware for specifying the server powered-by header
func Info(opts *InfoOpts) echo.MiddlewareFunc {
	return func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {

			// Set default values for opts.PoweredBy
			poweredBy := opts.PoweredBy
			if poweredBy == "" {
				poweredBy = "Surreal"
			}

			c.Response().Header().Set("X-Powered-By", poweredBy)

			return h(c)

		}
	}
}
