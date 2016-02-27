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

package server

import (
	"github.com/abcum/surreal/db"
	"github.com/labstack/echo"
	"golang.org/x/net/websocket"
)

func sock(c *echo.Context) error {

	ws := c.Socket()

	var msg string

	for {

		if err := websocket.Message.Receive(ws, &msg); err != nil {
			break
		}

		s, e := db.ExecuteString(msg)

		if e == nil {
			if err := websocket.Message.Send(ws, encode(show(s))); err != nil {
				break
			}
		}

		if e != nil {
			if err := websocket.Message.Send(ws, encode(oops(e))); err != nil {
				break
			}
		}

	}

	return nil

}
