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
	"sync"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/server/api"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

// Setup sets up the server for remote connections
func Setup(ctx cnf.Context) (e error) {

	var wg sync.WaitGroup

	wg.Add(2)

	// -------------------------------------------------------
	// REST handler
	// -------------------------------------------------------

	r := echo.New()

	r.Any("/", crud)

	r.SetDebug(ctx.Verbose)
	r.AutoIndex(false)
	r.SetHTTPErrorHandler(errors)

	r.Use(middleware.Gzip())
	r.Use(middleware.Logger())
	r.Use(middleware.Recover())
	r.Use(api.Head(&api.HeadOpts{}))
	r.Use(api.Type(&api.TypeOpts{}))
	r.Use(api.Cors(&api.CorsOpts{}))
	r.Use(api.Auth(&api.AuthOpts{}))

	go func() {
		defer wg.Done()
		log.Printf("Starting HTTP server on %s", ctx.Http)
		r.Run(ctx.Http)
	}()

	// -------------------------------------------------------
	// SOCK handler
	// -------------------------------------------------------

	s := echo.New()

	s.WebSocket("/", sock)

	r.SetDebug(ctx.Verbose)
	s.AutoIndex(false)
	s.SetHTTPErrorHandler(errors)

	s.Use(middleware.Gzip())
	s.Use(middleware.Logger())
	s.Use(middleware.Recover())
	s.Use(api.Head(&api.HeadOpts{}))
	s.Use(api.Type(&api.TypeOpts{}))
	s.Use(api.Cors(&api.CorsOpts{}))
	s.Use(api.Auth(&api.AuthOpts{}))

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
