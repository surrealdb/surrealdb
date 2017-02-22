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

	"io/ioutil"

	"github.com/sirupsen/logrus"
)

const (
	PanicLevel = logrus.PanicLevel
	FatalLevel = logrus.FatalLevel
	ErrorLevel = logrus.ErrorLevel
	WarnLevel  = logrus.WarnLevel
	InfoLevel  = logrus.InfoLevel
	DebugLevel = logrus.DebugLevel
)

var log *Logger

// Logger ...
type Logger struct {
	*logrus.Logger
}

func init() {
	log = New()
}

// Instance returns the underlying logger instance
func Instance() *logrus.Logger {
	return log.Logger
}

// SetLevel sets the level of the standard logger
func SetLevel(v string) {
	log.SetLevel(v)
}

// SetFormat sets the format of the standard logger
func SetFormat(v string) {
	log.SetFormat(v)
}

// SetOutput sets the output of the standard logger
func SetOutput(v string) {
	log.SetOutput(v)
}

// Debug logs a message at level Debug on the standard logger.
func Debug(v ...interface{}) {
	log.Debug(v...)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

// Debugln logs a message at level Debug on the standard logger.
func Debugln(v ...interface{}) {
	log.Debugln(v...)
}

// Error loggs a message at level Error on the standard logger.
func Error(v ...interface{}) {
	log.Error(v...)
}

// Errorf loggs a message at level Error on the standard logger.
func Errorf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}

// Errorln loggs a message at level Error on the standard logger.
func Errorln(v ...interface{}) {
	log.Errorln(v...)
}

// Fatal loggs a message at level Fatal on the standard logger.
func Fatal(v ...interface{}) {
	log.Fatal(v...)
}

// Fatalf loggs a message at level Fatal on the standard logger.
func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

// Fatalln loggs a message at level Fatal on the standard logger.
func Fatalln(v ...interface{}) {
	log.Fatalln(v...)
}

// Info loggs a message at level Info on the standard logger.
func Info(v ...interface{}) {
	log.Info(v...)
}

// Infof loggs a message at level Info on the standard logger.
func Infof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

// Infoln loggs a message at level Info on the standard logger.
func Infoln(v ...interface{}) {
	log.Infoln(v...)
}

// Panic loggs a message at level Panic on the standard logger.
func Panic(v ...interface{}) {
	log.Panic(v...)
}

// Panicf loggs a message at level Panic on the standard logger.
func Panicf(format string, v ...interface{}) {
	log.Panicf(format, v...)
}

// Panicln loggs a message at level Panic on the standard logger.
func Panicln(v ...interface{}) {
	log.Panicln(v...)
}

// Print loggs a message at level Print on the standard logger.
func Print(v ...interface{}) {
	log.Print(v...)
}

// Printf loggs a message at level Print on the standard logger.
func Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// Println loggs a message at level Print on the standard logger.
func Println(v ...interface{}) {
	log.Println(v...)
}

// Warn loggs a message at level Warn on the standard logger.
func Warn(v ...interface{}) {
	log.Warn(v...)
}

// Warnf loggs a message at level Warn on the standard logger.
func Warnf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

// Warnln loggs a message at level Warn on the standard logger.
func Warnln(v ...interface{}) {
	log.Warnln(v...)
}

// WithPrefix prepares a log entry with a prefix.
func WithPrefix(value interface{}) *logrus.Entry {
	return log.WithField("prefix", value)
}

// WithField prepares a log entry with a single data field.
func WithField(key string, value interface{}) *logrus.Entry {
	return log.WithField(key, value)
}

// WithFields prepares a log entry with multiple data fields.
func WithFields(fields map[string]interface{}) *logrus.Entry {
	return log.WithFields(fields)
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

// SetLevel sets the logging level of the logger instance.
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

// SetFormat sets the logging format of the logger instance.
func (l *Logger) SetFormat(v string) {
	switch v {
	case "json":
		l.Formatter = &logrus.JSONFormatter{
			TimestampFormat: time.RFC3339,
		}
	case "text":
		l.Formatter = &TextFormatter{
			TimestampFormat: time.RFC3339,
		}
	}
}

// SetOutput sets the logging output of the logger instance.
func (l *Logger) SetOutput(v string) {
	switch v {
	case "none":
		l.Out = ioutil.Discard
	case "stdout":
		l.Out = os.Stdout
	case "stderr":
		l.Out = os.Stderr
	}
}
