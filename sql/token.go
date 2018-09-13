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

func (p *parser) parseDefineTokenStatement() (stmt *DefineTokenStatement, err error) {

	stmt = &DefineTokenStatement{}

	stmt.What = &Ident{}

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.Kind, _, err = p.shouldBe(NAMESPACE, DATABASE, SCOPE); err != nil {
		return nil, err
	}

	if is(stmt.Kind, NAMESPACE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthNS); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, DATABASE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, SCOPE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
			return nil, err
		}
		if stmt.What, err = p.parseIdent(); err != nil {
			return nil, err
		}
	}

	for {

		tok, _, exi := p.mightBe(TYPE, VALUE)
		if !exi {
			break
		}

		if is(tok, TYPE) {
			if stmt.Type, err = p.parseAlgorithm(); err != nil {
				return nil, err
			}
		}

		if is(tok, VALUE) {
			if stmt.Code, err = p.parseBinary(); err != nil {
				return nil, err
			}
		}

	}

	if stmt.Type == "" {
		return nil, &ParseError{Found: "", Expected: []string{"TYPE"}}
	}

	if len(stmt.Code) == 0 {
		return nil, &ParseError{Found: "", Expected: []string{"VALUE"}}
	}

	return

}

func (p *parser) parseRemoveTokenStatement() (stmt *RemoveTokenStatement, err error) {

	stmt = &RemoveTokenStatement{}

	stmt.What = &Ident{}

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.Kind, _, err = p.shouldBe(NAMESPACE, DATABASE, SCOPE); err != nil {
		return nil, err
	}

	if is(stmt.Kind, NAMESPACE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthNS); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, DATABASE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
			return nil, err
		}
	}

	if is(stmt.Kind, SCOPE) {
		if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
			return nil, err
		}
		if stmt.What, err = p.parseIdent(); err != nil {
			return nil, err
		}
	}

	return

}
