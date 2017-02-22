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

package log

import (
	"time"

	"encoding/json"

	"github.com/Sirupsen/logrus"
)

type JSONFormatter struct {
	IgnoreFields    []string
	TimestampFormat string
}

func (f *JSONFormatter) ignore(field string) bool {
	for _, ignore := range f.IgnoreFields {
		if field == ignore {
			return true
		}
	}
	return false
}

func (f *JSONFormatter) include(field string) bool {
	for _, ignore := range f.IgnoreFields {
		if field == ignore {
			return false
		}
	}
	return true
}

func (f *JSONFormatter) Format(entry *logrus.Entry) (data []byte, err error) {

	if f.TimestampFormat == "" {
		f.TimestampFormat = time.RFC3339Nano
	}

	obj := make(map[string]interface{})

	obj["msg"] = entry.Message
	obj["time"] = entry.Time.Format(f.TimestampFormat)
	obj["level"] = entry.Level.String()

	for k, v := range entry.Data {
		if f.include(k) {
			switch x := v.(type) {
			case error:
				obj[k] = x.Error()
			default:
				obj[k] = x
			}
		}
	}

	data, err = json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	return append(data, '\n'), nil

}
