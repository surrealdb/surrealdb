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
	"fmt"
	"regexp"
	"strconv"
	"time"

	json "github.com/hjson/hjson-go"
)

func (p *parser) in(token Token, tokens []Token) bool {

	for _, t := range tokens {
		if token == t {
			return true
		}
	}

	return false

}

func (p *parser) is(token Token, tokens ...Token) bool {

	for _, t := range tokens {
		if token == t {
			return true
		}
	}

	return false

}

func (p *parser) contains(search string, strings []string) bool {

	for _, str := range strings {
		if str == search {
			return true
		}
	}

	return false

}

func (p *parser) declare(tok Token, lit string) (interface{}, error) {

	if val := p.hold(tok); val != nil {
		return val, nil
	}

	switch tok {

	case TRUE:
		return true, nil

	case FALSE:
		return false, nil

	case NULL:
		return &Null{}, nil

	case VOID:
		return &Void{}, nil

	case MISSING:
		return &Void{}, nil

	case EMPTY:
		return &Empty{}, nil

	case MUL:
		return &All{}, nil

	case QMARK:
		return &Any{}, nil

	case ASC:
		return &Asc{}, nil

	case DESC:
		return &Desc{}, nil

	case STRING:
		return &Value{lit}, nil

	case REGION:
		return &Value{lit}, nil

	case ID:
		return &Ident{lit}, nil

	case IDENT:
		return &Ident{lit}, nil

	case TABLE:
		return &Table{lit}, nil

	case NOW:
		return time.Now().UTC(), nil

	case DATE:
		return time.Parse("2006-01-02", lit)

	case TIME:
		return time.Parse(time.RFC3339, lit)

	case REGEX:
		return regexp.Compile(lit)

	case NUMBER:
		return strconv.ParseInt(lit, 10, 64)

	case DOUBLE:
		return strconv.ParseFloat(lit, 64)

	case DURATION:
		return time.ParseDuration(lit)

	case PARAM:
		if p, ok := p.v[lit]; ok {
			return p, nil
		}
		return &Param{lit}, nil

	case ARRAY:
		var j Array
		json.Unmarshal([]byte(lit), &j)
		if j == nil {
			return j, fmt.Errorf("Invalid JSON: %s", lit)
		}
		return j, nil

	case JSON:
		var j Object
		json.Unmarshal([]byte(lit), &j)
		if j == nil {
			return j, fmt.Errorf("Invalid JSON: %s", lit)
		}
		return j, nil

	}

	return lit, nil

}
