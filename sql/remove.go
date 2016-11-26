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

func (p *parser) parseRemoveStatement() (Statement, error) {

	if p.buf.txn {
		return nil, &TXError{}
	}

	// Inspect the next token.
	tok, _, err := p.shouldBe(NAMESPACE, DATABASE, LOGIN, TOKEN, SCOPE, TABLE, FIELD, INDEX, VIEW)

	switch tok {
	case NAMESPACE:
		return p.parseRemoveNamespaceStatement()
	case DATABASE:
		return p.parseRemoveDatabaseStatement()
	case LOGIN:
		return p.parseRemoveLoginStatement()
	case TOKEN:
		return p.parseRemoveTokenStatement()
	case SCOPE:
		return p.parseRemoveScopeStatement()
	case TABLE:
		return p.parseRemoveTableStatement()
	case FIELD:
		return p.parseRemoveFieldStatement()
	case INDEX:
		return p.parseRemoveIndexStatement()
	case VIEW:
		return p.parseRemoveViewStatement()
	default:
		return nil, err
	}

}
