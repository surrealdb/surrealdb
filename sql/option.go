// Copyright © 2016 SurrealDB Ltd.
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

func (p *parser) parseOptionStatement() (stmt *OptStatement, err error) {

	stmt = &OptStatement{What: true}

	_, lit, err := p.shouldBe(IDENT, STRING)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"name"}}
	}

	// The first part of a SET expression must
	// always be an identifier, specifying a
	// variable name to set.

	stmt.Name = lit

	// The next part might be a = operator,
	// which mist be followed by a TRUE or
	// FALSE boolean value.

	if _, _, exi := p.mightBe(EQ); exi {

		stmt.What, err = p.parseBool()
		if err != nil {
			return nil, err
		}

	}

	return

}
