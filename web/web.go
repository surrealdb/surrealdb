// Copyright © 2016 Abcum Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//,
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package web

import (
	"github.com/abcum/fibre"
	"github.com/abcum/fibre/mw"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/log"
)

// Setup sets up the server for remote connections
func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("web").Infof("Starting web server on %s", opts.Conn.Web)

	s := fibre.Server()

	routes(s)
	s.SetWait("15s")
	s.SetName("web")
	s.SetHTTPErrorHandler(errors)
	s.Logger().SetLogger(log.Instance())

	// Setup middleware

	s.Use(mw.Uniq()) // Add uniq id
	s.Use(mw.Fail()) // Catch panics
	s.Use(mw.Logs()) // Log requests
	s.Use(mw.Sock()) // Log requests
	s.Use(mw.Gzip()) // Gzip responses
	s.Use(mw.Cors()) // Add cors headers

	// Setup authentication

	s.Use(auth())

	// Check body size

	s.Use(mw.Size(&mw.SizeOpts{
		AllowedLength: 1 << 20,
	}))

	// Check body type

	s.Use(mw.Type(&mw.TypeOpts{
		AllowedContent: map[string]bool{
			"application/json":                  true,
			"application/cbor":                  true,
			"application/msgpack":               true,
			"application/octet-stream":          true,
			"application/x-www-form-urlencoded": true,
		},
	}))

	// Run the server

	if len(opts.Cert.Crt) == 0 || len(opts.Cert.Key) == 0 {
		s.Run(opts.Conn.Web)
	}

	if len(opts.Cert.Crt) != 0 && len(opts.Cert.Key) != 0 {
		s.Run(opts.Conn.Web, opts.Cert.Crt, opts.Cert.Key)
	}

	return nil

}

// Exit tears down the server gracefully
func Exit() {
	log.WithPrefix("web").Infof("Gracefully shutting down %s protocol", "web")
}
