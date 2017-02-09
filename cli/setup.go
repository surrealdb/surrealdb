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
	"regexp"
	"strings"
	"time"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/util/uuid"
)

func setup() {

	// --------------------------------------------------
	// DB
	// --------------------------------------------------

	// Ensure that the default
	// database options are set

	if opts.DB.Path == "" {
		opts.DB.Path = "memory"
	}

	if opts.DB.Base == "" {
		opts.DB.Base = "surreal"
	}

	if opts.DB.Code != "" {
		opts.DB.Key = []byte(opts.DB.Code)
	}

	if opts.DB.Time != "" {
		var err error
		if opts.DB.Sync, err = time.ParseDuration(opts.DB.Time); err != nil {
			log.Fatal("Specify a valid database sync time frequency")
		}
	}

	switch len(opts.DB.Key) {
	case 0, 16, 24, 32:
	default:
		log.Fatal("Specify a valid encryption key length. Valid key sizes are 16bit, 24bit, or 32bit.")
	}

	if opts.DB.Path != "memory" {
		if ok, _ := regexp.MatchString(`^(s3|gcs|logr|file|rixxdb|dendrodb)://(.+)$`, opts.DB.Path); !ok {
			log.Fatal("Specify a valid data store configuration path")
		}
	}

	if strings.HasPrefix(opts.DB.Cert.CA, "-----") {
		var err error
		var doc *os.File
		if doc, err = os.Create("db.ca"); err != nil {
			log.Fatal("Can not decode PEM encoded CA into db.ca")
		}
		doc.Write([]byte(opts.DB.Cert.CA))
		doc.Close()
		opts.Cert.Crt = "db.ca"
	}

	if strings.HasPrefix(opts.DB.Cert.Crt, "-----") {
		var err error
		var doc *os.File
		if doc, err = os.Create("db.key"); err != nil {
			log.Fatal("Can not decode PEM encoded certificate into db.crt")
		}
		doc.Write([]byte(opts.DB.Cert.Crt))
		doc.Close()
		opts.Cert.Crt = "db.crt"
	}

	if strings.HasPrefix(opts.DB.Cert.Key, "-----") {
		var err error
		var doc *os.File
		if doc, err = os.Create("db.crt"); err != nil {
			log.Fatal("Can not decode PEM encoded private key into db.key")
		}
		doc.Write([]byte(opts.DB.Cert.Key))
		doc.Close()
		opts.Cert.Crt = "db.key"
	}

	if opts.DB.Cert.CA != "" || opts.DB.Cert.Crt != "" || opts.DB.Cert.Key != "" {
		opts.DB.Cert.SSL = true
	}

	if opts.DB.Cert.CA == "" && opts.DB.Cert.SSL {
		log.Fatal("Specify a valid PEM encoded CA file.")
	}

	if opts.DB.Cert.Crt == "" && opts.DB.Cert.SSL {
		log.Fatal("Specify a valid PEM encoded certificate file.")
	}

	if opts.DB.Cert.Key == "" && opts.DB.Cert.SSL {
		log.Fatal("Specify a valid PEM encoded private key file.")
	}

	// --------------------------------------------------
	// Auth
	// --------------------------------------------------

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

	// --------------------------------------------------
	// Nodes
	// --------------------------------------------------

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

	// Convert string args to slices
	for _, v := range strings.Split(opts.Node.Attr, ",") {
		if c := strings.Trim(v, " "); c != "" {
			opts.Node.Tags = append(opts.Node.Tags, c)
		}
	}

	// Convert string args to slices
	for _, v := range strings.Split(opts.Cluster.Join, ",") {
		if c := strings.Trim(v, " "); c != "" {
			opts.Cluster.Peer = append(opts.Cluster.Peer, c)
		}
	}

	// --------------------------------------------------
	// Ports
	// --------------------------------------------------

	// Specify default port
	if opts.Port.Web == 0 {
		opts.Port.Web = 8000
	}

	// Specify default port
	if opts.Port.Tcp == 0 {
		opts.Port.Tcp = 33693
	}

	// Ensure port number is valid
	if opts.Port.Web < 0 || opts.Port.Web > 65535 {
		log.Fatal("Please specify a valid port number for --port-web")
	}

	// Ensure port number is valid
	if opts.Port.Tcp < 0 || opts.Port.Tcp > 65535 {
		log.Fatal("Please specify a valid port number for --port-tcp")
	}

	// Store the ports in host:port string format
	opts.Conn.Web = fmt.Sprintf(":%d", opts.Port.Web)
	opts.Conn.Tcp = fmt.Sprintf(":%d", opts.Port.Tcp)

	// --------------------------------------------------
	// Certs
	// --------------------------------------------------

	if strings.HasPrefix(opts.Cert.Crt, "-----") {
		var err error
		var doc *os.File
		if doc, err = os.Create("cert.crt"); err != nil {
			log.Fatal("Can not decode PEM encoded certificate into cert.crt")
		}
		doc.Write([]byte(opts.Cert.Crt))
		doc.Close()
		opts.Cert.Crt = "cert.crt"
	}

	if strings.HasPrefix(opts.Cert.Key, "-----") {
		var err error
		var doc *os.File
		if doc, err = os.Create("cert.key"); err != nil {
			log.Fatal("Can not decode PEM encoded private key into cert.key")
		}
		doc.Write([]byte(opts.Cert.Key))
		doc.Close()
		opts.Cert.Key = "cert.key"
	}

	// --------------------------------------------------
	// Logging
	// --------------------------------------------------

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

	cnf.Settings = opts

}
