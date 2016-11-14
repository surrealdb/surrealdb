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

func (p *parser) parseBeginStatement() (stmt *BeginStatement, err error) {

	stmt = &BeginStatement{}

	if _, _, _, err = p.o.get(AuthSC); err != nil {
		return nil, err
	}

	_, _, _ = p.mightBe(TRANSACTION)

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseCancelStatement() (stmt *CancelStatement, err error) {

	stmt = &CancelStatement{}

	if _, _, _, err = p.o.get(AuthSC); err != nil {
		return nil, err
	}

	_, _, _ = p.mightBe(TRANSACTION)

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseCommitStatement() (stmt *CommitStatement, err error) {

	stmt = &CommitStatement{}

	if _, _, _, err = p.o.get(AuthSC); err != nil {
		return nil, err
	}

	_, _, _ = p.mightBe(TRANSACTION)

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
