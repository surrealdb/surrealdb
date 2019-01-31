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

func (p *parser) parseLetStatement() (stmt *LetStatement, err error) {

	stmt = &LetStatement{}

	// The first part of a LET expression must
	// always be an identifier, specifying a
	// variable name to set.

	stmt.Name, err = p.parseParam()
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

	// If this query has any subqueries which
	// need to alter the database then mark
	// this query as a writeable statement.

	stmt.RW = p.buf.rw

	return

}
