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

func (p *Parser) parseDiff() (exp *DiffExpression, err error) {

	if _, _, err = p.shouldBe(DIFF); err != nil {
		return nil, err
	}

	exp = &DiffExpression{}

	tok, lit, err := p.shouldBe(JSON)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"json"}}
	}

	val, err := declare(tok, lit)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"json"}}
	}

	exp.JSON = val.(*JSONLiteral)

	return

}
