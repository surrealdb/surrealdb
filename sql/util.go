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

import (
	"encoding/json"
	"strconv"
	"time"
)

func in(token Token, tokens []Token) bool {

	for _, t := range tokens {
		if token == t {
			return true
		}
	}

	return false

}

func is(token Token, tokens ...Token) bool {

	for _, t := range tokens {
		if token == t {
			return true
		}
	}

	return false

}

func declare(tok Token, lit string) Expr {

	switch tok {

	case NULL:
		return &Null{}

	case ALL:
		return &Wildcard{}

	case ASC:
		return &DirectionLiteral{Val: true}

	case DESC:
		return &DirectionLiteral{Val: false}

	case IDENT:
		return &IdentLiteral{Val: lit}

	case DATE:
		t, _ := time.Parse("2006-01-02", lit)
		return &DatetimeLiteral{Val: t}

	case TIME:
		t, _ := time.Parse(time.RFC3339, lit)
		return &DatetimeLiteral{Val: t}

	case NANO:
		t, _ := time.Parse(time.RFC3339Nano, lit)
		return &DatetimeLiteral{Val: t}

	case TRUE:
		return &BooleanLiteral{Val: true}

	case FALSE:
		return &BooleanLiteral{Val: false}

	case STRING:
		return &StringLiteral{Val: lit}

	case REGION:
		return &StringLiteral{Val: lit}

	case NUMBER:
		i, _ := strconv.ParseInt(lit, 10, 64)
		return &NumberLiteral{Val: i}

	case DOUBLE:
		f, _ := strconv.ParseFloat(lit, 64)
		return &DoubleLiteral{Val: f}

	case DURATION:
		return &DurationLiteral{Val: 0}

	case JSON:
		var j interface{}
		b := []byte(lit)
		json.Unmarshal(b, &j)
		return &JSONLiteral{Val: j}

	}

	return lit

}
