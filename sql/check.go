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

package sql

func checkExpression(allowed map[string]bool, expr Fields, grps Groups) error {

	if len(grps) > 0 {

	skip:
		for _, e := range expr {

			for _, g := range grps {

				// If the expression in the SELECT
				// clause is a field, then check to
				// see if it exists in the GROUP BY.

				if i, ok := g.Expr.(*Ident); ok {
					if e.Field == i.ID {
						continue skip
					}
				}

				// Otherwise if the expression in
				// the SELECT clause is a function
				// then check to see if it is an
				// aggregate function.

				if f, ok := e.Expr.(*FuncExpression); ok {
					if ok = allowed[f.Name]; ok {
						continue skip
					}
				}

			}

			// If the expression in the SELECT
			// clause isn't an aggregate function
			// and isn't specified in the GROUP BY
			// clause, then raise an error.

			return &GroupError{found: e.Field}

		}

	}

	return nil

}
