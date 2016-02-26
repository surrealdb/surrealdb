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
	"mime"
	"sort"

	"github.com/labstack/echo"
)

// TypeOpts defines options for the Head middleward
type TypeOpts struct {
	AllowedContent []string
}

// Type defines middleware for checking the request content-type
func Type(opts *TypeOpts) echo.MiddlewareFunc {
	return func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {

			// Set default values for opts.AllowedContent
			allowedContent := opts.AllowedContent
			if len(allowedContent) == 0 {
				allowedContent = []string{echo.ApplicationJSON}
			}

			// Extract the Content-Type header
			header := c.Request().Header.Get(echo.ContentType)
			cotype, _, _ := mime.ParseMediaType(header)

			// Sort and search opts.AllowedContent types
			sort.Strings(allowedContent)
			i := sort.SearchStrings(allowedContent, cotype)

			if c.Request().ContentLength == 0 {
				return h(c)
			}

			if c.Request().ContentLength >= 1 {
				if i < len(allowedContent) {
					return h(c)
				}
			}

			return code(415)

		}
	}
}
