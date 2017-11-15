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

	"golang.org/x/crypto/ssh/terminal"
)

const clear = ansi.Reset

var (
	isTerminal bool
	isColoured bool
)

func init() {
	isTerminal = terminal.IsTerminal(int(os.Stdout.Fd()))
	isColoured = isTerminal && (runtime.GOOS != "windows")
}

type TextFormatter struct {
	IgnoreFields    []string
	TimestampFormat string
}

func (f *TextFormatter) ignore(field string) bool {
	for _, ignore := range f.IgnoreFields {
		if field == ignore {
			return true
		}
	}
	return false
}

func (f *TextFormatter) include(field string) bool {
	for _, ignore := range f.IgnoreFields {
		if field == ignore {
			return false
		}
	}
	return true
}

func (f *TextFormatter) Format(entry *logrus.Entry) ([]byte, error) {

	if f.TimestampFormat == "" {
		f.TimestampFormat = time.RFC3339Nano
	}

	var keys []string = make([]string, 0)

	for k := range entry.Data {
		if f.include(k) {
			if k != "prefix" {
				keys = append(keys, k)
			}
		}
	}

	sort.Strings(keys)

	b := &bytes.Buffer{}

	switch isColoured {
	case false:
		f.printBasic(b, entry, keys)
	case true:
		f.printColored(b, entry, keys)
	}

	b.WriteByte('\n')

	return b.Bytes(), nil

}

func (f *TextFormatter) printField(b *bytes.Buffer, key string, value interface{}) {
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

func (f *TextFormatter) printBasic(b *bytes.Buffer, entry *logrus.Entry, keys []string) {

	f.printField(b, "time", entry.Time.Format(f.TimestampFormat))
	f.printField(b, "level", entry.Level.String())
	f.printField(b, "msg", entry.Message)
	for _, key := range keys {
		f.printField(b, key, entry.Data[key])
	}

}

func (f *TextFormatter) printColored(b *bytes.Buffer, entry *logrus.Entry, keys []string) {

	var color string
	var prefix string

	switch entry.Level {
	case logrus.InfoLevel:
		color = ansi.Green
	case logrus.WarnLevel:
		color = ansi.Yellow
	case logrus.ErrorLevel:
		color = ansi.Red
	case logrus.FatalLevel:
		color = ansi.Red
	case logrus.PanicLevel:
		color = ansi.Red
	default:
		color = ansi.Blue
	}

	level := strings.ToUpper(entry.Level.String())[0:4]

	if value, ok := entry.Data["prefix"]; ok {
		prefix = fmt.Sprint(" ", ansi.Cyan, value, ":", clear)
	}

	fmt.Fprintf(b, "%s[%s]%s %s%+5s%s%s %s", ansi.LightBlack, entry.Time.Format(f.TimestampFormat), clear, color, level, clear, prefix, entry.Message)

	for _, k := range keys {
		fmt.Fprintf(b, " %s%s%s=%+v", color, k, clear, entry.Data[k])
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
