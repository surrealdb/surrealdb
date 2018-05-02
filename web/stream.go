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
	"sync"

	"github.com/abcum/surreal/log"
	"github.com/sirupsen/logrus"
)

var streamer *stream

type stream struct {
	wss sync.Map
}

type socket struct {
	quit chan struct{}
	msgs chan interface{}
}

func init() {
	streamer = &stream{}
	log.Instance().AddHook(streamer)
}

func (h *stream) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *stream) Fire(entry *logrus.Entry) error {

	streamer.wss.Range(func(key, val interface{}) bool {

		ws := val.(*socket)

		select {
		case <-ws.quit:
			close(ws.msgs)
			break
		case ws.msgs <- h.Format(entry):
			break
		}

		return true

	})

	return nil

}

func (h *stream) Format(entry *logrus.Entry) interface{} {

	var keys = make(map[string]interface{})
	var json = make(map[string]interface{})

	for k, v := range entry.Data {
		if k != "prefix" && k != "ctx" {
			keys[k] = v
		}
	}

	json["keys"] = keys

	json["time"] = entry.Time

	json["level"] = entry.Level.String()

	json["message"] = entry.Message

	json["prefix"], _ = entry.Data["prefix"]

	return json

}
