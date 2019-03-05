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
	"net"
	"os"
	"path"
	"regexp"
	"strings"

	"encoding/pem"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/util/rand"
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

	switch len(opts.DB.Key) {
	case 0, 16, 24, 32:
	default:
		log.Fatal("Specify a valid encryption key length. Valid key sizes are 16bit, 24bit, or 32bit.")
	}

	if opts.DB.Path != "memory" {
		if ok, _ := regexp.MatchString(`^(s3|gcs|logr|file|dendrodb)://(.+)$`, opts.DB.Path); !ok {
			log.Fatalf("Invalid path %s. Specify a valid data store configuration path", opts.DB.Path)
		}
	}

	if opts.DB.Proc.Size == 0 {
		opts.DB.Proc.Size = 5
	}

	if opts.DB.Proc.Size < 0 {
		log.Fatal("Specify a valid data file size policy. Valid sizes are greater than 0 and are specified in MB.")
	}

	if opts.DB.Cert.CA != "" || opts.DB.Cert.Crt != "" || opts.DB.Cert.Key != "" {

		opts.DB.Cert.SSL = true

		if dec, _ := pem.Decode([]byte(opts.DB.Cert.CA)); dec == nil || dec.Type != "CERTIFICATE" {
			log.Fatal("Specify a valid PEM encoded CA file.")
		}

		if dec, _ := pem.Decode([]byte(opts.DB.Cert.Crt)); dec == nil || dec.Type != "CERTIFICATE" {
			log.Fatal("Specify a valid PEM encoded certificate file.")
		}

		if dec, _ := pem.Decode([]byte(opts.DB.Cert.Key)); dec == nil || dec.Type != "RSA PRIVATE KEY" {
			log.Fatal("Specify a valid PEM encoded private key file.")
		}

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
		opts.Auth.Pass = string(rand.New(20))
	}

	// Ensure that login as
	// root can only be from
	// specified ip addresses

	for _, cidr := range opts.Auth.Addr {
		_, subn, err := net.ParseCIDR(cidr)
		if err != nil {
			log.Fatalf("Invalid cidr %s. Please specify a valid CIDR address for --auth-addr", cidr)
		}
		opts.Auth.Nets = append(opts.Auth.Nets, subn)
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
		opts.Node.UUID = opts.Node.Name + "-" + uuid.New().String()
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
		log.Fatalf("Invalid port %d. Please specify a valid port number for --port-web", opts.Port.Web)
	}

	// Ensure port number is valid
	if opts.Port.Tcp < 0 || opts.Port.Tcp > 65535 {
		log.Fatalf("Invalid port %d. Please specify a valid port number for --port-tcp", opts.Port.Tcp)
	}

	// Store the ports in host:port string format
	opts.Conn.Web = fmt.Sprintf("%s:%d", opts.Node.Host, opts.Port.Web)
	opts.Conn.Tcp = fmt.Sprintf("%s:%d", opts.Node.Host, opts.Port.Tcp)

	// --------------------------------------------------
	// Certs
	// --------------------------------------------------

	if strings.HasPrefix(opts.Cert.Crt, "-----") {
		var err error
		var doc *os.File
		var out string = path.Join(os.TempDir(), "surreal.crt")
		if doc, err = os.Create(out); err != nil {
			log.Fatalf("Can not decode PEM encoded certificate into %s", out)
		}
		doc.Write([]byte(opts.Cert.Crt))
		doc.Close()
		opts.Cert.Crt = out
	}

	if strings.HasPrefix(opts.Cert.Key, "-----") {
		var err error
		var doc *os.File
		var out string = path.Join(os.TempDir(), "surreal.key")
		if doc, err = os.Create(out); err != nil {
			log.Fatalf("Can not decode PEM encoded private key into %s", out)
		}
		doc.Write([]byte(opts.Cert.Key))
		doc.Close()
		opts.Cert.Key = out
	}

	// --------------------------------------------------
	// Logging
	// --------------------------------------------------

	var chk map[string]bool

	// Setup a default logging
	// hook for cli output

	logger := &log.DefaultHook{}

	// Ensure that the specified
	// logging level is allowed

	if opts.Logging.Level != "" {

		chk = map[string]bool{
			"trace": true,
			"debug": true,
			"info":  true,
			"warn":  true,
			"error": true,
			"fatal": true,
			"panic": true,
		}

		if _, ok := chk[opts.Logging.Level]; !ok {
			log.Fatal("Incorrect log level specified")
		}

		logger.SetLevel(opts.Logging.Level)

	}

	// Ensure that the specified
	// logging format is allowed

	if opts.Logging.Format != "" {

		chk = map[string]bool{
			"text": true,
			"json": true,
		}

		if _, ok := chk[opts.Logging.Format]; !ok {
			log.Fatal("Incorrect log format specified")
		}

		logger.SetFormat(opts.Logging.Format)

	}

	// Ensure that the specified
	// logging output is allowed

	if opts.Logging.Output != "" {

		chk = map[string]bool{
			"none":   true,
			"stdout": true,
			"stderr": true,
		}

		if _, ok := chk[opts.Logging.Output]; !ok {
			log.Fatal("Incorrect log output specified")
		}

		logger.SetOutput(opts.Logging.Output)

	}

	// Add the default logging hook
	// to the logger instance

	log.Hook(logger)

	// Enable global options object

	cnf.Settings = opts

}
