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
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

type DefaultHook struct {
	w io.Writer
	l []logrus.Level
	f logrus.Formatter
}

func (h *DefaultHook) Levels() []logrus.Level {
	return h.l
}

func (h *DefaultHook) Fire(entry *logrus.Entry) error {
	bit, err := h.f.Format(entry)
	if err != nil {
		return err
	}
	_, err = h.w.Write(bit)
	return err
}

// SetLevel sets the logging level of the logger instance.
func (h *DefaultHook) SetLevel(v string) {
	switch v {
	case "trace":
		h.l = TraceLevels
	case "debug":
		h.l = DebugLevels
	case "info":
		h.l = InfoLevels
	case "warn":
		h.l = WarnLevels
	case "error":
		h.l = ErrorLevels
	case "fatal":
		h.l = FatalLevels
	case "panic":
		h.l = PanicLevels
	}
}

// SetOutput sets the logging output of the logger instance.
func (h *DefaultHook) SetOutput(v string) {
	switch v {
	case "none":
		h.w = ioutil.Discard
	case "stdout":
		h.w = os.Stdout
	case "stderr":
		h.w = os.Stderr
	}
}

// SetFormat sets the logging format of the logger instance.
func (h *DefaultHook) SetFormat(v string) {
	switch v {
	case "json":
		h.f = &JSONFormatter{
			IgnoreFields: []string{
				"ctx",
				"vars",
			},
			TimestampFormat: time.RFC3339,
		}
	case "text":
		h.f = &TextFormatter{
			IgnoreFields: []string{
				"ctx",
				"vars",
			},
			TimestampFormat: time.RFC3339,
		}
	}
}
