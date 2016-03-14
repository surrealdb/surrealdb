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

// SizeOpts defines options for the Size middleward
type SizeOpts struct {
	Maximum int64
}

// Size defines middleware for limiting request size
func Size(opts *SizeOpts) echo.MiddlewareFunc {
	return func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {

			// Set default values for opts.Maximum
			maximum := opts.Maximum
			if maximum == 0 {
				maximum = 1000000
			}

			if c.Request().ContentLength <= maximum {
				return h(c)
			}

			return echo.NewHTTPError(413)

		}
	}
}
