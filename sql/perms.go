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

func (p *parser) parsePerms() (exp *PermExpression, err error) {

	exp = &PermExpression{
		Select: false,
		Create: false,
		Update: false,
		Delete: false,
		Relate: false,
	}

	tok, _, err := p.shouldBe(FOR, NONE, FULL, WHERE)
	if err != nil {
		return exp, err
	}

	if p.is(tok, NONE, FULL, WHERE) {

		var expr Expr

		switch tok {
		case FULL:
			expr = true
		case NONE:
			expr = false
		case WHERE:
			if expr, err = p.parseExpr(); err != nil {
				return exp, err
			}
		}

		exp.Select = expr
		exp.Create = expr
		exp.Update = expr
		exp.Delete = expr
		exp.Relate = expr

		return

	}

	if p.is(tok, FOR) {

		for {

			var expr Expr
			var when []Token

			for {
				tok, _, err := p.shouldBe(SELECT, CREATE, UPDATE, DELETE, RELATE)
				if err != nil {
					return exp, err
				}
				when = append(when, tok)
				if _, _, exi := p.mightBe(COMMA); !exi {
					break
				}
			}

			tok, _, err := p.shouldBe(FULL, NONE, WHERE)
			if err != nil {
				return exp, err
			}

			switch tok {
			case FULL:
				expr = true
			case NONE:
				expr = false
			case WHERE:
				if expr, err = p.parseExpr(); err != nil {
					return exp, err
				}
			}

			for _, w := range when {
				switch w {
				case SELECT:
					exp.Select = expr
				case CREATE:
					exp.Create = expr
				case UPDATE:
					exp.Update = expr
				case DELETE:
					exp.Delete = expr
				case RELATE:
					exp.Relate = expr
				}
			}

			if _, _, exi := p.mightBe(FOR); !exi {
				break
			}

		}

	}

	return

}
