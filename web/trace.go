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

package web

import (
	"context"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/trace"
	"google.golang.org/api/option"
)

var client *trace.Client

// tracer returns a middleware function for stackdriver trace monitoring.
func tracer() fibre.MiddlewareFunc {

	var e error
	var s trace.SamplingPolicy
	var p string = cnf.Settings.Logging.Google.Project
	var c string = cnf.Settings.Logging.Google.Credentials

	// If no project id has been set
	// then attempt to pull this from
	// machine metadata if on GCE.

	if p == "" {
		if p, e = metadata.ProjectID(); e != nil {
			return fibre.MiddlewareSkip
		}
	}

	// Connect to Stackdriver using a
	// credentials file if one has been
	// specified, or metadata if not.

	switch c {
	case "":
		client, e = trace.NewClient(
			context.Background(),
			p,
		)
	default:
		client, e = trace.NewClient(
			context.Background(),
			p,
			option.WithServiceAccountFile(c),
		)
	}

	if e != nil {
		return fibre.MiddlewareSkip
	}

	// Attempt to setup the Stackdriver
	// client policy so that a fraction
	// of requests are sent to google.

	if s, e = trace.NewLimitedSampler(1, 5); e != nil {
		return fibre.MiddlewareSkip
	}

	client.SetSamplingPolicy(s)

	return func(h fibre.HandlerFunc) fibre.HandlerFunc {
		return func(c *fibre.Context) error {

			if c.Request().Header().Get("Upgrade") == "websocket" {
				return h(c)
			}

			span := client.SpanFromRequest(c.Request().Request)

			span.SetLabel("http/id", c.Get("id").(string))

			ctx := trace.NewContext(c.Context(), span)

			c = c.WithContext(ctx)

			defer span.Finish()

			return h(c)

		}
	}
}
