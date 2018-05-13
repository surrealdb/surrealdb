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
	"time"

	"net/http"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/util/rand"
)

const cookie = "surreal"

func uniq(val *http.Cookie) string {
	if val != nil && len(val.Value) == 64 {
		return val.Value
	}
	return rand.String(64)
}

func sess() fibre.MiddlewareFunc {
	return func(h fibre.HandlerFunc) fibre.HandlerFunc {
		return func(c *fibre.Context) (err error) {

			val, _ := c.Request().Cookie(cookie)
			crt := len(cnf.Settings.Cert.Crt) != 0
			key := len(cnf.Settings.Cert.Key) != 0

			val = &http.Cookie{
				Name:     cookie,
				Value:    uniq(val),
				Secure:   (crt && key),
				HttpOnly: true,
				Expires:  time.Now().Add(365 * 24 * time.Hour),
			}

			c.Response().Header().Set("Set-Cookie", val.String())

			c.Set(varKeyCook, val.Value)

			return h(c)

		}
	}
}
