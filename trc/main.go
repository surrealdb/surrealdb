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

package trc

import (
	"context"

	"go.opencensus.io/trace"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/log"

	"contrib.go.opencensus.io/exporter/stackdriver"
)

func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("log").Infof("Starting open tracing framework")

	var exporter *stackdriver.Exporter

	opt := stackdriver.Options{
		ProjectID: "surreal-io",
		OnError:   func(error) {},
	}

	exporter, err = stackdriver.NewExporter(opt)
	if err != nil {
		return err
	}

	err = exporter.StartMetricsExporter()
	if err != nil {
		return err
	}

	trace.ApplyConfig(trace.Config{
		DefaultSampler: trace.AlwaysSample(),
	})

	trace.RegisterExporter(exporter)

	return

}

func Start(ctx context.Context, name string) (context.Context, *trace.Span) {
	return trace.StartSpan(ctx, name)
}
