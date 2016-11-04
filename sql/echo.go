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

func (p *parser) parseEcho() (exp Token, err error) {

	// The next token that we expect to see is a
	// RETURN token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(RETURN); exi {

		exp, _, err = p.shouldBe(ID, NONE, INFO, BOTH, DIFF, BEFORE, AFTER)
		if err != nil {
			return 0, err
		}

	}

	return

}
