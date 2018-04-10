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

package db

import (
	"context"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func (d *document) yield(ctx context.Context, stm sql.Statement, output sql.Token) (interface{}, error) {

	switch stm := stm.(type) {

	case *sql.SelectStatement:

		var doc *data.Doc

		for _, v := range stm.Expr {
			if _, ok := v.Expr.(*sql.All); ok {
				doc = d.current
				break
			}
		}

		if doc == nil {
			doc = data.New()
		}

		for _, e := range stm.Expr {

			switch v := e.Expr.(type) {
			case *sql.All:
				break
			default:

				// If the query has a GROUP BY expression
				// then let's check if this is an aggregate
				// function, and if it is then pass the
				// first argument directly through.

				if len(stm.Group) > 0 {
					if f, ok := e.Expr.(*sql.FuncExpression); ok && f.Aggr {
						v, err := d.i.e.fetch(ctx, f.Args[0], d.current)
						if err != nil {
							return nil, err
						}
						doc.Set(v, f.String())
						continue
					}
				}

				// Otherwise treat the field normally, and
				// calculate the value to be inserted into
				// the final output document.

				v, err := d.i.e.fetch(ctx, v, d.current)
				if err != nil {
					return nil, err
				}

				switch v {
				case d.current:
					doc.Set(nil, e.Field)
				default:
					doc.Set(v, e.Field)
				}

			}

		}

		return doc.Data(), nil

	default:

		switch output {
		default:
			return nil, nil
		case sql.DIFF:
			return d.diff().Data(), nil
		case sql.AFTER:
			return d.current.Data(), nil
		case sql.BEFORE:
			return d.initial.Data(), nil
		case sql.BOTH:
			return map[string]interface{}{
				"after":  d.current.Data(),
				"before": d.initial.Data(),
			}, nil
		}

	}

}
