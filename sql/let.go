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

import "time"

func (p *parser) parseLetStatement() (stmt *LetStatement, err error) {

	stmt = &LetStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthTB); err != nil {
		return nil, err
	}

	// The first part of a LET expression must
	// always be an identifier, specifying a
	// variable name to set.

	_, stmt.Name, err = p.shouldBe(IDENT)
	if err != nil {
		return nil, err
	}

	// The next query part must always be a =
	// operator, as this is a LET expression
	// and not a binary expression.

	_, _, err = p.shouldBe(EQ)
	if err != nil {
		return nil, err
	}

	// The next query part can be any expression
	// including a parenthesised expression or a
	// binary expression so handle accordingly.

	stmt.What, err = p.parseExpr()
	if err != nil {
		return nil, err
	}

	// If the defined paramater is a basic type,
	// then instead of defining it at a later
	// stage, convert it to that type here.

	switch stmt.What.(type) {
	case bool, int64, float64, string:
		p.v[stmt.Name] = stmt.What
	case []interface{}, map[string]interface{}:
		p.v[stmt.Name] = stmt.What
	case time.Time, time.Duration:
		p.v[stmt.Name] = stmt.What
	case *Null, *Void, *Empty, *Table, *Thing, *Param:
		p.v[stmt.Name] = stmt.What
	}

	// Check that we have reached the end of the
	// statement with either a ';' or EOF.

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
