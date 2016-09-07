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
	"regexp"
	"strconv"
	"time"
)

func (p *Parser) in(token Token, tokens []Token) bool {

	for _, t := range tokens {
		if token == t {
			return true
		}
	}

	return false

}

func (p *Parser) is(token Token, tokens ...Token) bool {

	for _, t := range tokens {
		if token == t {
			return true
		}
	}

	return false

}

func (p *Parser) contains(search string, strings []string) bool {

	for _, str := range strings {
		if str == search {
			return true
		}
	}

	return false

}

func (p *Parser) declare(tok Token, lit string) (interface{}, error) {

	if val := p.hold(tok); val != nil {
		return val, nil
	}

	switch tok {

	case TRUE:
		return true, nil

	case FALSE:
		return false, nil

	case STRING:
		return lit, nil

	case REGION:
		return lit, nil

	case NULL:
		return &Null{}, nil

	case VOID:
		return &Void{}, nil

	case MISSING:
		return &Void{}, nil

	case EMPTY:
		return &Empty{}, nil

	case ALL:
		return &All{}, nil

	case ASC:
		return &Asc{}, nil

	case DESC:
		return &Desc{}, nil

	case ID:
		return &Ident{lit}, nil

	case IDENT:
		return &Ident{lit}, nil

	case NOW:
		return time.Now(), nil

	case DATE:
		return time.Parse("2006-01-02", lit)

	case TIME:
		return time.Parse(time.RFC3339, lit)

	case REGEX:
		return regexp.Compile(lit)

	case NUMBER:
		return strconv.ParseFloat(lit, 64)

	case DOUBLE:
		return strconv.ParseFloat(lit, 64)

	case DURATION:
		return time.ParseDuration(lit)

	case ARRAY:
		var j []interface{}
		json.Unmarshal([]byte(lit), &j)
		if j == nil {
			return j, fmt.Errorf("Invalid JSON: %s", lit)
		}
		return j, nil

	case JSON:
		var j map[string]interface{}
		json.Unmarshal([]byte(lit), &j)
		if j == nil {
			return j, fmt.Errorf("Invalid JSON: %s", lit)
		}
		return j, nil

	case PARAM:
		if p, ok := p.v[lit]; ok {
			return p, nil
		}
		return nil, fmt.Errorf("Param %s is not defined", lit)

	}

	return lit, nil

}
