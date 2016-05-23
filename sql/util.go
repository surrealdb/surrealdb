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
	"fmt"
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

func contains(search string, strings []string) bool {

	for _, str := range strings {
		if str == search {
			return true
		}
	}

	return false

}

func declare(tok Token, lit string) (Expr, error) {

	switch tok {

	case NULL:
		return &Null{}, nil

	case VOID:
		return &Void{}, nil

	case EMPTY:
		return &Empty{}, nil

	case ALL:
		return &Wildcard{}, nil

	case ASC:
		return &Asc{}, nil

	case DESC:
		return &Desc{}, nil

	case ID:
		return &IdentLiteral{Val: lit}, nil

	case IDENT:
		return &IdentLiteral{Val: lit}, nil

	case NOW:
		return &DatetimeLiteral{Val: time.Now()}, nil

	case DATE:
		t, err := time.Parse("2006-01-02", lit)
		return &DatetimeLiteral{Val: t}, err

	case TIME:
		t, err := time.Parse(time.RFC3339, lit)
		return &DatetimeLiteral{Val: t}, err

	case TRUE:
		return &BooleanLiteral{Val: true}, nil

	case FALSE:
		return &BooleanLiteral{Val: false}, nil

	case STRING:
		return &StringLiteral{Val: lit}, nil

	case REGION:
		return &StringLiteral{Val: lit}, nil

	case NUMBER:
		i, err := strconv.ParseInt(lit, 10, 64)
		return &NumberLiteral{Val: i}, err

	case DOUBLE:
		f, err := strconv.ParseFloat(lit, 64)
		return &DoubleLiteral{Val: f}, err

	case DURATION:
		t, err := time.ParseDuration(lit)
		return &DurationLiteral{Val: t}, err

	case ARRAY:
		var j ArrayLiteral
		json.Unmarshal([]byte(lit), &j.Val)
		if j.Val == nil {
			return &j, fmt.Errorf("Invalid JSON: %s", lit)
		}
		return &j, nil

	case JSON:
		var j JSONLiteral
		json.Unmarshal([]byte(lit), &j.Val)
		if j.Val == nil {
			return &j, fmt.Errorf("Invalid JSON: %s", lit)
		}
		return &j, nil

	}

	return lit, nil

}
