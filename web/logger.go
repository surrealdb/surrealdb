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
	"github.com/abcum/surreal/cnf"
)

func logger(c *fibre.Context) (err error) {

	if err := c.Upgrade(); err != nil {
		return err
	}

	if c.Get("auth").(*cnf.Auth).Kind != cnf.AuthKV {
		return fibre.NewHTTPError(401)
	}

	ws := &socket{
		quit: make(chan struct{}),
		msgs: make(chan interface{}),
	}

	streamer.wss.Store(c.Get("id"), ws)

	for v := range ws.msgs {
		err := c.Socket().SendJSON(v)
		if err != nil {
			ws.quit <- struct{}{}
		}
	}

	streamer.wss.Delete(c.Get("id"))

	return nil

}
