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
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/abcum/fibre"

	"github.com/sirupsen/logrus"

	"github.com/abcum/surreal/util/build"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/errorreporting"
	"cloud.google.com/go/logging"
)

var proj = os.Getenv("PROJECT")

type StackdriverLogger struct {
	name   string
	client *logging.Client
	logger *logging.Logger
	errors *errorreporting.Client
}

func NewStackDriver() *StackdriverLogger {

	var err error

	ctx := context.Background()

	hook := new(StackdriverLogger)

	conf := errorreporting.Config{
		ServiceName:    "surreal",
		ServiceVersion: build.GetInfo().Ver,
	}

	// If no project id has been set
	// then attempt to pull this from
	// machine metadata if on GCE.

	if len(proj) == 0 {
		if proj, err = metadata.ProjectID(); err != nil {
			log.Fatalf("Failed to connect to Stackdriver: %v", err)
		}
	}

	// Connect to Stackdriver logging
	// using the project name retrieved
	// from the machine metadata.

	hook.client, err = logging.NewClient(ctx, proj)
	if err != nil {
		log.Fatalf("Failed to connect to Stackdriver: %v", err)
	}

	// Connect to Stackdriver errors
	// using the project name retrieved
	// from the machine metadata.

	hook.errors, err = errorreporting.NewClient(ctx, proj, conf)
	if err != nil {
		log.Fatalf("Failed to connect to Stackdriver: %v", err)
	}

	// Attempt to ping the Stackdriver
	// endpoint to ensure the settings
	// and authentication are correct.

	err = hook.client.Ping(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to Stackdriver: %v", err)
	}

	hook.logger = hook.client.Logger("surreal")

	return hook

}

func (h *StackdriverLogger) Levels() []logrus.Level {
	switch log.GetLevel() {
	case TraceLevel:
		return TraceLevels
	case DebugLevel:
		return DebugLevels
	case InfoLevel:
		return InfoLevels
	case WarnLevel:
		return WarnLevels
	case ErrorLevel:
		return ErrorLevels
	case FatalLevel:
		return FatalLevels
	case PanicLevel:
		return PanicLevels
	default:
		return DebugLevels
	}
}

func (h *StackdriverLogger) Fire(entry *logrus.Entry) error {

	// If we receive an error, fatal, or
	// panic - then log the error to GCE
	// with a full stack trace.

	if entry.Level <= logrus.ErrorLevel {

		e := errorreporting.Entry{
			Error: fmt.Errorf("%s", entry.Message),
		}

		for _, v := range entry.Data {
			switch i := v.(type) {
			case *http.Request:
				e.Req = i
			case *fibre.Context:
				e.Req = i.Request().Request
			}
		}

		h.errors.Report(e)

	}

	// Otherwise just log the entry to
	// Stackdriver, and attach any http
	// request data to it if available.

	e := logging.Entry{
		Timestamp: entry.Time,
		Labels:    make(map[string]string, len(entry.Data)),
		Payload:   entry.Message,
		Severity:  logging.ParseSeverity(entry.Level.String()),
	}

	if v, ok := entry.Data["trace"].(string); ok {
		e.Trace = fmt.Sprintf("projects/%s/traces/%s", proj, v)
	}

	if v, ok := entry.Data["span"].(string); ok {
		e.SpanID = v
	}

	if p, ok := entry.Data["prefix"]; ok && p == "sql" {
		e.Payload = map[string]interface{}{
			"sql":  entry.Message,
			"vars": entry.Data["vars"],
		}
	}

	for k, v := range entry.Data {
		switch i := v.(type) {
		default:
			e.Labels[k] = fmt.Sprintf("%v", i)
		case *http.Request:
			e.HTTPRequest = &logging.HTTPRequest{
				Request:  i,
				RemoteIP: i.RemoteAddr,
			}
		case *fibre.Context:
			e.HTTPRequest = &logging.HTTPRequest{
				RemoteIP:     i.IP().String(),
				Request:      i.Request().Request,
				Status:       i.Response().Status(),
				RequestSize:  i.Request().Size(),
				ResponseSize: i.Response().Size(),
				Latency:      time.Since(i.Request().Start()),
			}
		}
	}

	h.logger.Log(e)

	return nil

}
