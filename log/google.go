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
	"time"

	"runtime/debug"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/util/build"

	"github.com/Sirupsen/logrus"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/errors"
	"cloud.google.com/go/logging"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/api/monitoredres"
)

type HookGoogle struct {
	name        string
	project     string
	credentials string
	level       logrus.Level
	levels      []logrus.Level
	errclient   *errors.Client
	logclient   *logging.Client
	logbuffer   *logging.Logger
}

func NewGoogleHook(level, name, project, credentials string) (hook *HookGoogle, err error) {

	hook = &HookGoogle{}

	// Ensure that we only send the
	// specified log levels to the
	// Google Stackdriver endpoint.

	switch level {
	case "debug":
		hook.level = logrus.DebugLevel
	case "info":
		hook.level = logrus.InfoLevel
	case "warning":
		hook.level = logrus.WarnLevel
	case "error":
		hook.level = logrus.ErrorLevel
	case "fatal":
		hook.level = logrus.FatalLevel
	case "panic":
		hook.level = logrus.PanicLevel
	default:
		return nil, fmt.Errorf("Please specify a valid google logging level")
	}

	for l := logrus.PanicLevel; l <= hook.level; l++ {
		hook.levels = append(hook.levels, l)
	}

	// Specify the log name that all
	// logs should be stored under in
	// Google Stackdriver.

	hook.name = name

	// If no project id has been set
	// then attempt to pull this from
	// machine metadata if on GCE.

	if project == "" {

		if project, err = metadata.ProjectID(); err != nil {
			return nil, err
		}

	}

	// Otherwise set the log name to
	// the project name which has been
	// specified on the command line.

	hook.project = project

	// Connect to Stackdriver using a
	// credentials file if one has been
	// specified, or metadata if not.

	switch credentials {
	case "":
		hook.logclient, err = logging.NewClient(
			context.Background(),
			hook.project,
		)
	default:
		hook.logclient, err = logging.NewClient(
			context.Background(),
			hook.project,
			option.WithServiceAccountFile(credentials),
		)
	}

	if err != nil {
		return nil, err
	}

	// Attempt to ping the Stackdriver
	// endpoint to ensure the settings
	// and authentication are correct.

	err = hook.logclient.Ping(context.Background())

	if err != nil {
		return nil, err
	}

	// Attempt to ping the Stackdriver
	// endpoint to ensure the settings
	// and authentication are correct.

	hook.errclient, err = errors.NewClient(
		context.Background(),
		hook.project,
		hook.name,
		build.GetInfo().Ver,
		true,
	)

	if err != nil {
		return nil, err
	}

	// Setup the asynchronous buffering
	// logger, which we can use to send
	// logs to the Stackdriver client.

	hook.logbuffer = hook.logclient.Logger(
		hook.name,
		logging.CommonResource(&monitoredres.MonitoredResource{
			Type: "logging_log",
		}),
	)

	return hook, err

}

func (h *HookGoogle) Levels() []logrus.Level {
	return h.levels
}

func (h *HookGoogle) Fire(entry *logrus.Entry) error {

	// If we receive an error, fatal, or
	// panic - then log the error to GCE
	// with a full stack trace.

	switch entry.Level {
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		ctx := context.Background()
		err := fmt.Sprintf("%s\n%s", entry.Message, debug.Stack())
		go func() {
			for _, v := range entry.Data {
				switch i := v.(type) {
				case *http.Request:
					h.errclient.Report(ctx, i, err)
					return
				case *fibre.Context:
					h.errclient.Report(ctx, i.Request().Request, err)
					return
				}
			}
		}()
	}

	// Otherwise just log the error to
	// GCE as a log entry, and attach any
	// http request data to it.

	msg := logging.Entry{
		Timestamp: entry.Time,
		Labels:    make(map[string]string),
		Payload:   entry.Message,
		Severity:  logging.ParseSeverity(entry.Level.String()),
	}

	for k, v := range entry.Data {
		switch i := v.(type) {
		default:
			msg.Labels[k] = fmt.Sprintf("%v", i)
		case *http.Request:
			msg.HTTPRequest = &logging.HTTPRequest{
				Request:  i,
				RemoteIP: i.RemoteAddr,
			}
		case *fibre.Context:
			msg.HTTPRequest = &logging.HTTPRequest{
				RemoteIP:     i.IP().String(),
				Request:      i.Request().Request,
				Status:       i.Response().Status(),
				RequestSize:  i.Request().Size(),
				ResponseSize: i.Response().Size(),
				Latency:      time.Since(i.Request().Start()),
			}
		}
	}

	h.logbuffer.Log(msg)

	return nil

}
