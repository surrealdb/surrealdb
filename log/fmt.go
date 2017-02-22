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
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/mgutz/ansi"

	"github.com/Sirupsen/logrus"
)

const clear = ansi.Reset

var (
	baseTimestamp time.Time
	isTerminal    bool
)

func init() {
	baseTimestamp = time.Now()
	isTerminal = logrus.IsTerminal(os.Stdout)
}

func miniTS() int {
	return int(time.Since(baseTimestamp) / time.Second)
}

type TextFormatter struct {
	// Set to true to bypass checking for a TTY before outputting colors.
	ForceColors bool

	// Force disabling colors.
	DisableColors bool

	// Disable timestamp logging. useful when output is redirected to logging
	// system that already adds timestamps.
	DisableTimestamp bool

	// Enable logging of just the time passed since beginning of execution.
	ShortTimestamp bool

	// Timestamp format to use for display when a full timestamp is printed.
	TimestampFormat string

	// The fields are sorted by default for a consistent output. For applications
	// that log extremely frequently and don't use the JSON formatter this may not
	// be desired.
	DisableSorting bool
}

func (f *TextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var keys []string = make([]string, 0, len(entry.Data))
	for k := range entry.Data {
		if k != "prefix" {
			keys = append(keys, k)
		}
	}

	if !f.DisableSorting {
		sort.Strings(keys)
	}

	b := &bytes.Buffer{}

	prefixFieldClashes(entry.Data)

	isColorTerminal := isTerminal && (runtime.GOOS != "windows")
	isColored := (f.ForceColors || isColorTerminal) && !f.DisableColors

	timestampFormat := f.TimestampFormat
	if timestampFormat == "" {
		timestampFormat = time.Stamp
	}
	if isColored {
		f.printColored(b, entry, keys, timestampFormat)
	} else {
		if !f.DisableTimestamp {
			f.appendKeyValue(b, "time", entry.Time.Format(timestampFormat))
		}
		f.appendKeyValue(b, "level", entry.Level.String())
		if entry.Message != "" {
			f.appendKeyValue(b, "msg", entry.Message)
		}
		for _, key := range keys {
			f.appendKeyValue(b, key, entry.Data[key])
		}
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

func (f *TextFormatter) printColored(b *bytes.Buffer, entry *logrus.Entry, keys []string, timestampFormat string) {
	var levelColor string
	var levelText string
	switch entry.Level {
	case logrus.InfoLevel:
		levelColor = ansi.Green
	case logrus.WarnLevel:
		levelColor = ansi.Yellow
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		levelColor = ansi.Red
	default:
		levelColor = ansi.Blue
	}

	if entry.Level != logrus.WarnLevel {
		levelText = strings.ToUpper(entry.Level.String())
	} else {
		levelText = "WARN"
	}

	prefix := ""
	prefixValue, ok := entry.Data["prefix"]
	if ok {
		prefix = fmt.Sprint(" ", ansi.Cyan, prefixValue, ":", clear)
	}

	if f.ShortTimestamp {
		fmt.Fprintf(b, "%s[%04d]%s %s%+5s%s%s %s", ansi.LightBlack, miniTS(), clear, levelColor, levelText, clear, prefix, entry.Message)
	} else {
		fmt.Fprintf(b, "%s[%s]%s %s%+5s%s%s %s", ansi.LightBlack, entry.Time.Format(timestampFormat), clear, levelColor, levelText, clear, prefix, entry.Message)
	}
	for _, k := range keys {
		v := entry.Data[k]
		fmt.Fprintf(b, " %s%s%s=%+v", levelColor, k, clear, v)
	}
}

func needsQuoting(text string) bool {
	for _, ch := range text {
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '.') {
			return false
		}
	}
	return true
}

func (f *TextFormatter) appendKeyValue(b *bytes.Buffer, key string, value interface{}) {
	b.WriteString(key)
	b.WriteByte('=')

	switch value := value.(type) {
	case string:
		if needsQuoting(value) {
			b.WriteString(value)
		} else {
			fmt.Fprintf(b, "%q", value)
		}
	case error:
		errmsg := value.Error()
		if needsQuoting(errmsg) {
			b.WriteString(errmsg)
		} else {
			fmt.Fprintf(b, "%q", value)
		}
	default:
		fmt.Fprint(b, value)
	}

	b.WriteByte(' ')
}

func prefixFieldClashes(data logrus.Fields) {
	_, ok := data["time"]
	if ok {
		data["fields.time"] = data["time"]
	}
	_, ok = data["msg"]
	if ok {
		data["fields.msg"] = data["msg"]
	}
	_, ok = data["level"]
	if ok {
		data["fields.level"] = data["level"]
	}
}
