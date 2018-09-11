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

package fncs

import (
	"context"
	"fmt"
	"strings"

	"github.com/abcum/surreal/util/ints"
	"github.com/abcum/surreal/util/slug"
	"github.com/abcum/surreal/util/text"
)

func stringConcat(ctx context.Context, args ...interface{}) (interface{}, error) {
	var str string
	for _, v := range args {
		str = str + fmt.Sprint(v)
	}
	return str, nil
}

func stringContains(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	p, _ := ensureString(args[1])
	return strings.Contains(s, p), nil
}

func stringEndsWith(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	p, _ := ensureString(args[1])
	return strings.HasSuffix(s, p), nil
}

func stringFormat(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 0, 1:
		// Not enough arguments, so just ignore
	default:
		s, _ := ensureString(args[0])
		return fmt.Sprintf(s, args[1:]...), nil
	}
	return nil, nil
}

func stringIncludes(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	p, _ := ensureString(args[1])
	return strings.Contains(s, p), nil
}

func stringJoin(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 0, 1:
		// Not enough arguments, so just ignore
	default:
		var a []string
		j, _ := ensureString(args[0])
		for _, v := range args[1:] {
			if v != nil {
				a = append(a, fmt.Sprint(v))
			}
		}
		return strings.Join(a, j), nil
	}
	return nil, nil
}

func stringLength(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	return float64(len(s)), nil
}

func stringLevenshtein(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	c, _ := ensureString(args[1])
	return float64(text.Levenshtein(s, c)), nil
}

func stringLowercase(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	return strings.ToLower(s), nil
}

func stringRepeat(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	if c, ok := ensureInt(args[1]); ok {
		return strings.Repeat(s, int(c)), nil
	}
	return s, nil
}

func stringReplace(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	o, _ := ensureString(args[1])
	n, _ := ensureString(args[2])
	return strings.Replace(s, o, n, -1), nil
}

func stringReverse(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r), nil
}

func stringSearch(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	p, _ := ensureString(args[1])
	return float64(strings.Index(s, p)), nil
}

func stringSlice(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	b, bk := ensureInt(args[1])
	e, ek := ensureInt(args[2])
	f := ints.Min(len(s), int(b+e))
	if bk && ek {
		return s[b:f], nil
	} else if bk {
		return s[b:], nil
	} else if ek {
		return s[:f], nil
	}
	return s, nil
}

func stringSlug(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 1:
		s, _ := ensureString(args[0])
		return slug.Make(s), nil
	case 2:
		s, _ := ensureString(args[0])
		l, _ := ensureString(args[1])
		return slug.MakeLang(s, l), nil
	}
	return nil, nil
}

func stringSplit(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	p, _ := ensureString(args[1])
	return strings.Split(s, p), nil
}

func stringStartsWith(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	p, _ := ensureString(args[1])
	return strings.HasPrefix(s, p), nil
}

func stringSubstr(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	b, bk := ensureInt(args[1])
	e, ek := ensureInt(args[2])
	f := ints.Min(len(s), int(e))
	if bk && ek {
		return s[b:f], nil
	} else if bk {
		return s[b:], nil
	} else if ek {
		return s[:f], nil
	}
	return s, nil
}

func stringTrim(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	return strings.TrimSpace(s), nil
}

func stringUppercase(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	return strings.ToUpper(s), nil
}

func stringWords(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	return strings.Fields(s), nil
}
