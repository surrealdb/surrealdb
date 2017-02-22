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
	"log/syslog"

	"github.com/Sirupsen/logrus"
)

type HookSyslog struct {
	host     string
	protocol string
	endpoint *syslog.Writer
}

func NewSyslogHook(level, protocol, host, priority, tag string) (hook *HookSyslog, err error) {

	hook = &HookSyslog{}

	var endpoint *syslog.Writer
	var severity syslog.Priority

	// Convert the passed priority to
	// one of the expected syslog
	// severity levels.

	switch priority {
	case "debug":
		severity = syslog.LOG_DEBUG
	case "info":
		severity = syslog.LOG_INFO
	case "notice":
		severity = syslog.LOG_NOTICE
	case "warning":
		severity = syslog.LOG_WARNING
	case "err":
		severity = syslog.LOG_ERR
	case "crit":
		severity = syslog.LOG_CRIT
	case "alert":
		severity = syslog.LOG_ALERT
	case "emerg":
		severity = syslog.LOG_EMERG
	default:
		return nil, fmt.Errorf("Please specify a valid syslog priority")
	}

	// Attempt to dial the syslog
	// endpoint, or exit if there is
	// a problem connecting.

	if endpoint, err = syslog.Dial(protocol, host, severity, tag); err != nil {
		return nil, err
	}

	// Finish setting up the logrus
	// hook with the configuration
	// options which were specified.

	hook.host = host
	hook.protocol = protocol
	hook.endpoint = endpoint

	return hook, err

}

func (h *HookSyslog) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *HookSyslog) Fire(entry *logrus.Entry) error {

	line := entry.Message

	for k, v := range entry.Data {
		line += fmt.Sprintf(" %s=%v", k, v)
	}

	switch entry.Level {
	case logrus.PanicLevel:
		h.endpoint.Crit(line)
	case logrus.FatalLevel:
		h.endpoint.Crit(line)
	case logrus.ErrorLevel:
		h.endpoint.Err(line)
	case logrus.WarnLevel:
		h.endpoint.Warning(line)
	case logrus.InfoLevel:
		h.endpoint.Notice(line)
	case logrus.DebugLevel:
		h.endpoint.Info(line)
	}

	return nil

}
