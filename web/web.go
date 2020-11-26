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

	log.WithPrefix("web").Infof("Starting web server on %s", opts.Conn)

	s := fibre.Server()

	routes(s)
	s.Pprof()
	s.SetName("web")
	s.SetIdleTimeout("5s")
	s.SetReadTimeout("30s")
	s.SetWriteTimeout("60s")
	s.SetHTTPErrorHandler(errors)
	s.Logger().SetLogger(log.Instance())

	// Setup middleware

	s.Use(mw.Uniq()) // Add uniq id
	s.Use(mw.Fail()) // Catch panics
	s.Use(mw.Logs()) // Log requests

	// Add cors headers

	s.Use(mw.Cors(&mw.CorsOpts{
		AllowedOrigin: "=",
		AllowedMethods: []string{
			"GET",
			"PUT",
			"POST",
			"PATCH",
			"DELETE",
			"TRACE",
			"OPTIONS",
		},
		AllowedHeaders: []string{
			"Accept",
			"Authorization",
			"Content-Type",
			"Origin",
			"NS",
			"DB",
			"ID",
		},
		AccessControlMaxAge: 1800,
	}))

	// Check body size

	s.Use(mw.Size(&mw.SizeOpts{
		AllowedLength: 1 << 20, // 1mb
	}))

	// Setup authentication

	s.Use(auth())

	// Setup live queries

	s.Use(live())

	// Redirect non-https

	s.Use(mw.Secure(&mw.SecureOpts{
		RedirectHTTP: len(opts.Cert.Crt) != 0 || len(opts.Cert.Key) != 0,
	}))

	// Log successful start

	log.WithPrefix("web").Infof("Started web server on %s", opts.Conn)

	// Run the server

	if len(opts.Cert.Crt) == 0 || len(opts.Cert.Key) == 0 {
		s.Run(opts.Conn)
	}

	if len(opts.Cert.Crt) != 0 && len(opts.Cert.Key) != 0 {
		s.Run(opts.Conn, opts.Cert.Crt, opts.Cert.Key)
	}

	return nil

}

// Exit tears down the server gracefully
func Exit(opts *cnf.Options) (err error) {
	log.WithPrefix("web").Infof("Shutting down web server on %s", opts.Conn)
	return
}
