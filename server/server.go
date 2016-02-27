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

package server

import (
	"log"
	"strings"
	"sync"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/server/api"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/thoas/stats"
)

var stat *stats.Stats

// Setup sets up the server for remote connections
func Setup(ctx cnf.Context) (e error) {

	var wg sync.WaitGroup

	wg.Add(3)

	// -------------------------------------------------------
	// Stats
	// -------------------------------------------------------

	stat = stats.New()

	// -------------------------------------------------------
	// GUI handler
	// -------------------------------------------------------

	w := echo.New()

	w.Get("/info", info)
	w.Static("/", "tpl")

	w.SetDebug(ctx.Verbose)
	w.AutoIndex(false)
	w.SetHTTPErrorHandler(errors)

	w.Use(stat.Handler)
	w.Use(middleware.Gzip())
	w.Use(middleware.Logger())
	w.Use(middleware.Recover())
	w.Use(api.Head(&api.HeadOpts{}))
	w.Use(api.Type(&api.TypeOpts{}))
	w.Use(api.Cors(&api.CorsOpts{}))
	w.Use(api.Auth(&api.AuthOpts{}))

	w.Use(middleware.BasicAuth(func(u, p string) bool {
		log.Println(ctx.Auth)
		if ctx.Auth != "" {
			a := strings.SplitN(ctx.Auth, ":", 2)
			return a[0] == u && a[1] == p
		}
		return true
	}))

	// -------------------------------------------------------
	// REST handler
	// -------------------------------------------------------

	r := echo.New()

	r.Any("/", crud)

	r.SetDebug(ctx.Verbose)
	r.AutoIndex(false)
	r.SetHTTPErrorHandler(errors)

	r.Use(stat.Handler)
	r.Use(middleware.Gzip())
	r.Use(middleware.Logger())
	r.Use(middleware.Recover())
	r.Use(api.Head(&api.HeadOpts{}))
	r.Use(api.Type(&api.TypeOpts{}))
	r.Use(api.Cors(&api.CorsOpts{}))
	r.Use(api.Auth(&api.AuthOpts{}))

	// -------------------------------------------------------
	// SOCK handler
	// -------------------------------------------------------

	s := echo.New()

	s.WebSocket("/", sock)

	r.SetDebug(ctx.Verbose)
	s.AutoIndex(false)
	s.SetHTTPErrorHandler(errors)

	s.Use(stat.Handler)
	s.Use(middleware.Gzip())
	s.Use(middleware.Logger())
	s.Use(middleware.Recover())
	s.Use(api.Head(&api.HeadOpts{}))
	s.Use(api.Type(&api.TypeOpts{}))
	s.Use(api.Cors(&api.CorsOpts{}))
	s.Use(api.Auth(&api.AuthOpts{}))

	// -------------------------------------------------------
	// Start servers
	// -------------------------------------------------------

	go func() {
		defer wg.Done()
		log.Printf("Starting Web server on %s", ctx.Port)
		w.Run(ctx.Port)
	}()

	go func() {
		defer wg.Done()
		log.Printf("Starting HTTP server on %s", ctx.Http)
		r.Run(ctx.Http)
	}()

	go func() {
		defer wg.Done()
		log.Printf("Starting SOCK server on %s", ctx.Sock)
		s.Run(ctx.Sock)
	}()

	// -------------------------------------------------------
	// Start server
	// -------------------------------------------------------

	wg.Wait()

	return nil

}
