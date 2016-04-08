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

package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/util/uuid"
)

func setup() {

	if opts.Auth.Auth != "" {

		if opts.Auth.User != "" {
			log.Fatal("Specify only --auth or --auth-user")
		}

		if opts.Auth.Pass != "" {
			log.Fatal("Specify only --auth or --auth-pass")
		}

		both := strings.SplitN(opts.Auth.Auth, ":", 2)

		if len(both) == 2 {
			opts.Auth.User = both[0]
			opts.Auth.Pass = both[1]
		}

	}

	// Ensure that security
	// is enabled by default

	if opts.Auth.User == "" {
		opts.Auth.User = "root"
	}

	if opts.Auth.Pass == "" {
		opts.Auth.Pass = "root"
	}

	// Ensure that the default
	// database is defined

	if opts.Store == "" {
		opts.Store = "127.0.0.1:26257"
	}

	// Ensure that the default
	// ports are defined

	if opts.Port.Http == 0 {
		opts.Port.Http = 8000
	}

	if opts.Port.Raft == 0 {
		opts.Port.Raft = 33693
	}

	// Ensure that the default
	// node details are defined

	if opts.Node.Host == "" {
		opts.Node.Host, _ = os.Hostname()
	}

	if opts.Node.Name == "" {
		opts.Node.Name = opts.Node.Host
	}

	if opts.Node.UUID == "" {
		opts.Node.UUID = opts.Node.Name + "-" + uuid.NewV4().String()
	}

	// Ensure the defined ports
	// are within range

	if opts.Port.Http == opts.Port.Raft {
		log.Fatal("Defined ports must be different")
	}

	if opts.Port.Http > 65535 {
		log.Fatal("Please specify a valid port number for --port-http")
	}

	if opts.Port.Raft > 65535 {
		log.Fatal("Please specify a valid port number for --port-raft")
	}

	// Define the listen string
	// with host:port format

	opts.Conn.Http = fmt.Sprintf(":%d", opts.Port.Http)
	opts.Conn.Raft = fmt.Sprintf(":%d", opts.Port.Raft)

	// Ensure that string args
	// are converted to slices

	opts.Node.Tags = strings.Split(opts.Node.Attr, ",")
	for k, v := range opts.Node.Tags {
		opts.Node.Tags[k] = strings.Trim(v, " ")
	}

	opts.Cluster.Peer = strings.Split(opts.Cluster.Join, ",")
	for k, v := range opts.Cluster.Peer {
		opts.Cluster.Peer[k] = strings.Trim(v, " ")
	}

	// Ensure that the specified
	// logging level is allowed

	if opts.Logging.Level != "" {

		chk := map[string]bool{
			"debug":   true,
			"info":    true,
			"warning": true,
			"error":   true,
			"fatal":   true,
			"panic":   true,
		}

		if _, ok := chk[opts.Logging.Level]; !ok {
			log.Fatal("Incorrect log level specified")
		}

		log.SetLevel(opts.Logging.Level)

	}

	// Ensure that the specified
	// logging output is allowed

	if opts.Logging.Output != "" {

		chk := map[string]bool{
			"stdout": true,
			"stderr": true,
		}

		if _, ok := chk[opts.Logging.Output]; !ok {
			log.Fatal("Incorrect log output specified")
		}

		log.SetOutput(opts.Logging.Output)

	}

	// Ensure that the specified
	// logging format is allowed

	if opts.Logging.Format != "" {

		chk := map[string]bool{
			"text": true,
			"json": true,
		}

		if _, ok := chk[opts.Logging.Format]; !ok {
			log.Fatal("Incorrect log format specified")
		}

		log.SetFormat(opts.Logging.Format)

	}

}
