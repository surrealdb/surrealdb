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
	"os"
	"time"

	"github.com/Sirupsen/logrus"
)

var log *Logger
var old logrus.Level

// Logger ...
type Logger struct {
	*logrus.Logger
}

func init() {
	log = New()
}

func Instance() *logrus.Logger {
	return log.Logger
}

func Debug(v ...interface{}) {
	log.Debug(v...)
}

func Debugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func Debugln(v ...interface{}) {
	log.Debugln(v...)
}

func Error(v ...interface{}) {
	log.Error(v...)
}

func Errorf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}

func Errorln(v ...interface{}) {
	log.Errorln(v...)
}

func Fatal(v ...interface{}) {
	log.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func Fatalln(v ...interface{}) {
	log.Fatalln(v...)
}

func Info(v ...interface{}) {
	log.Info(v...)
}

func Infof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

func Infoln(v ...interface{}) {
	log.Infoln(v...)
}

func Panic(v ...interface{}) {
	log.Panic(v...)
}

func Panicf(format string, v ...interface{}) {
	log.Panicf(format, v...)
}

func Panicln(v ...interface{}) {
	log.Panicln(v...)
}

func Print(v ...interface{}) {
	log.Print(v...)
}

func Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func Println(v ...interface{}) {
	log.Println(v...)
}

func Warn(v ...interface{}) {
	log.Warn(v...)
}

func Warnf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

func Warnln(v ...interface{}) {
	log.Warnln(v...)
}

func WithField(key string, value interface{}) *logrus.Entry {
	return log.WithField(key, value)
}

func WithFields(fields map[string]interface{}) *logrus.Entry {
	return log.WithFields(fields)
}

func SetLevel(v string) {
	log.SetLevel(v)
}

func SetFormat(v string) {
	log.SetFormat(v)
}

func SetOutput(v string) {
	log.SetOutput(v)
}

// New returns a new Logger instance.
func New() *Logger {

	return &Logger{
		&logrus.Logger{
			Out:   os.Stderr,
			Level: logrus.ErrorLevel,
			Hooks: logrus.LevelHooks{},
			Formatter: &TextFormatter{
				TimestampFormat: time.RFC3339,
			},
		},
	}
}

// SetLevel sets the logging level.
func (l *Logger) SetLevel(v string) {
	switch v {
	case "debug", "DEBUG":
		l.Level = logrus.DebugLevel
	case "info", "INFO":
		l.Level = logrus.InfoLevel
	case "warning", "WARNING":
		l.Level = logrus.WarnLevel
	case "error", "ERROR":
		l.Level = logrus.ErrorLevel
	case "fatal", "FATAL":
		l.Level = logrus.FatalLevel
	case "panic", "PANIC":
		l.Level = logrus.PanicLevel
	}
}

// SetFormat sets the logging format.
func (l *Logger) SetFormat(v string) {
	switch v {
	case "json":
		l.Formatter = &logrus.JSONFormatter{}
	case "text":
		l.Formatter = &TextFormatter{
			TimestampFormat: time.RFC3339,
		}
	}
}

// SetOutput sets the logging output.
func (l *Logger) SetOutput(v string) {
	switch v {
	case "stdout":
		l.Out = os.Stdout
	case "stderr":
		l.Out = os.Stderr
	}
}
