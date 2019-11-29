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
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	PanicLevel = logrus.PanicLevel
	FatalLevel = logrus.FatalLevel
	ErrorLevel = logrus.ErrorLevel
	WarnLevel  = logrus.WarnLevel
	InfoLevel  = logrus.InfoLevel
	DebugLevel = logrus.DebugLevel
	TraceLevel = logrus.TraceLevel
)

var (
	PanicLevels = []logrus.Level{
		PanicLevel,
	}
	FatalLevels = []logrus.Level{
		PanicLevel,
		FatalLevel,
	}
	ErrorLevels = []logrus.Level{
		PanicLevel,
		FatalLevel,
		ErrorLevel,
	}
	WarnLevels = []logrus.Level{
		PanicLevel,
		FatalLevel,
		ErrorLevel,
		WarnLevel,
	}
	InfoLevels = []logrus.Level{
		PanicLevel,
		FatalLevel,
		ErrorLevel,
		WarnLevel,
		InfoLevel,
	}
	DebugLevels = []logrus.Level{
		PanicLevel,
		FatalLevel,
		ErrorLevel,
		WarnLevel,
		InfoLevel,
		DebugLevel,
	}
	TraceLevels = []logrus.Level{
		PanicLevel,
		FatalLevel,
		ErrorLevel,
		WarnLevel,
		InfoLevel,
		DebugLevel,
		TraceLevel,
	}
)

var log *Logger

// Logger ...
type Logger struct {
	*logrus.Logger
}

func init() {
	log = &Logger{
		logrus.New(),
	}
}

// Instance returns the underlying logger instance
func Instance() *logrus.Logger {
	return log.Logger
}

// Hook adds a logging hook to the logger instance
func Hook(hook logrus.Hook) {
	log.AddHook(hook)
}

// IsPanic returns whether panic level logs are enabled
func IsPanic() bool {
	return log.IsLevelEnabled(PanicLevel)
}

// IsFatal returns whether fatal level logs are enabled
func IsFatal() bool {
	return log.IsLevelEnabled(FatalLevel)
}

// IsError returns whether error level logs are enabled
func IsError() bool {
	return log.IsLevelEnabled(ErrorLevel)
}

// IsWarn returns whether warning level logs are enabled
func IsWarn() bool {
	return log.IsLevelEnabled(WarnLevel)
}

// IsInfo returns whether info level logs are enabled
func IsInfo() bool {
	return log.IsLevelEnabled(InfoLevel)
}

// IsDebug returns whether debug level logs are enabled
func IsDebug() bool {
	return log.IsLevelEnabled(DebugLevel)
}

// IsTrace returns whether trace level logs are enabled
func IsTrace() bool {
	return log.IsLevelEnabled(TraceLevel)
}

// SetLevel sets the logging level of the logger instance.
func SetLevel(v string) {
	switch v {
	case "trace":
		log.Logger.SetLevel(TraceLevel)
	case "debug":
		log.Logger.SetLevel(DebugLevel)
	case "info":
		log.Logger.SetLevel(InfoLevel)
	case "warn":
		log.Logger.SetLevel(WarnLevel)
	case "error":
		log.Logger.SetLevel(ErrorLevel)
	case "fatal":
		log.Logger.SetLevel(FatalLevel)
	case "panic":
		log.Logger.SetLevel(PanicLevel)
	}
}

// SetOutput sets the logging output of the logger instance.
func SetOutput(v string) {
	switch v {
	case "none":
		log.Logger.SetOutput(ioutil.Discard)
	case "stdout":
		log.Logger.SetOutput(os.Stdout)
	case "stderr":
		log.Logger.SetOutput(os.Stderr)
	case "stackdriver":
		log.Logger.SetOutput(ioutil.Discard)
		log.Logger.AddHook(NewStackDriver())
	}
}

// SetFormat sets the logging format of the logger instance.
func SetFormat(v string) {
	switch v {
	case "json":
		log.Logger.SetFormatter(&JSONFormatter{
			IgnoreFields: []string{
				"ctx",
				"vars",
			},
			TimestampFormat: time.RFC3339,
		})
	case "text":
		log.Logger.SetFormatter(&TextFormatter{
			IgnoreFields: []string{
				"ctx",
				"vars",
			},
			TimestampFormat: time.RFC3339,
		})
	}
}

func Display(v ...interface{}) {
	if isTerminal {
		fmt.Print(v...)
	}
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
