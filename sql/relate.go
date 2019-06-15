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

func (p *parser) parseRelateStatement() (stmt *RelateStatement, err error) {

	tok := ILLEGAL

	stmt = &RelateStatement{}

	if stmt.From, err = p.parseWhat(); err != nil {
		return nil, err
	}

	if tok, _, err = p.shouldBe(OEDGE, IEDGE); err != nil {
		return nil, err
	}

	if tok == OEDGE {

		stmt.From = stmt.From

		if stmt.Type, err = p.parseTable(); err != nil {
			return nil, err
		}

		if _, _, err = p.shouldBe(OEDGE); err != nil {
			return nil, err
		}

		if stmt.With, err = p.parseWhat(); err != nil {
			return nil, err
		}

	}

	if tok == IEDGE {

		stmt.With = stmt.From

		if stmt.Type, err = p.parseTable(); err != nil {
			return nil, err
		}

		if _, _, err = p.shouldBe(IEDGE); err != nil {
			return nil, err
		}

		if stmt.From, err = p.parseWhat(); err != nil {
			return nil, err
		}

	}

	_, _, stmt.Uniq = p.mightBe(UNIQUE)

	if stmt.Data, err = p.parseData(); err != nil {
		return nil, err
	}

	if stmt.Echo, err = p.parseEcho(AFTER); err != nil {
		return nil, err
	}

	if stmt.Timeout, err = p.parseTimeout(); err != nil {
		return nil, err
	}

	if stmt.Parallel, err = p.parseParallel(); err != nil {
		return nil, err
	}

	return

}
