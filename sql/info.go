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

func (p *parser) parseInfoStatement() (stmt *InfoStatement, err error) {

	stmt = &InfoStatement{}

	if _, _, err = p.shouldBe(FOR); err != nil {
		return nil, err
	}

	if stmt.Kind, _, err = p.shouldBe(ALL, NAMESPACE, DATABASE, SCOPE, TABLE, NS, DB); err != nil {
		return nil, err
	}

	if is(stmt.Kind, ALL) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthKV); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, NAMESPACE, NS) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthNS); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, DATABASE, DB) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthDB); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, SCOPE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthDB); err != nil {
			return nil, err
		}
		if stmt.What, err = p.parseIdent(); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, TABLE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthDB); err != nil {
			return nil, err
		}
		if stmt.What, err = p.parseIdent(); err != nil {
			return nil, err
		}
	}

	return

}
