// Copyright © 2016 SurrealDB Ltd.
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
	"net/mail"
	"regexp"
)

const (
	sAlpha       string = "^[a-zA-Z]+$"
	sAlphanum    string = "^[a-zA-Z0-9]+$"
	sAscii       string = "^[\x00-\x7F]+$"
	sDomain      string = `^([a-zA-Z0-9_]{1}[a-zA-Z0-9_-]{0,62}){1}(\.[a-zA-Z0-9_]{1}[a-zA-Z0-9_-]{0,62})*[\._]?$`
	sHexadecimal string = "^[0-9a-fA-F]+$"
	sLatitude    string = "^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$"
	sLongitude   string = "^[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$"
	sNumeric     string = "^[0-9]+$"
	sSemver      string = "^v?(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)(-(0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(\\.(0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*)?(\\+[0-9a-zA-Z-]+(\\.[0-9a-zA-Z-]+)*)?$"
	sUUID        string = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

var (
	rAlpha       = regexp.MustCompile(sAlpha)
	rAlphanum    = regexp.MustCompile(sAlphanum)
	rAscii       = regexp.MustCompile(sAscii)
	rDomain      = regexp.MustCompile(sDomain)
	rHexadecimal = regexp.MustCompile(sHexadecimal)
	rLatitude    = regexp.MustCompile(sLatitude)
	rLongitude   = regexp.MustCompile(sLongitude)
	rNumeric     = regexp.MustCompile(sNumeric)
	rSemver      = regexp.MustCompile(sSemver)
	rUUID        = regexp.MustCompile(sUUID)
)

func isAlpha(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rAlpha.MatchString(val), nil
	}
	return false, nil
}

func isAlphanum(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rAlphanum.MatchString(val), nil
	}
	return false, nil
}

func isAscii(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rAscii.MatchString(val), nil
	}
	return false, nil
}

func isDomain(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rDomain.MatchString(val), nil
	}
	return false, nil
}

func isEmail(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); len(val) > 0 && ok {
		pse, err := mail.ParseAddress(val)
		return err == nil && val == pse.Address, nil
	}
	return false, nil
}

func isHexadecimal(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rHexadecimal.MatchString(val), nil
	}
	return false, nil
}

func isLatitude(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rLatitude.MatchString(val), nil
	}
	return false, nil
}

func isLongitude(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rLongitude.MatchString(val), nil
	}
	return false, nil
}

func isNumeric(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rNumeric.MatchString(val), nil
	}
	return false, nil
}

func isSemver(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rSemver.MatchString(val), nil
	}
	return false, nil
}

func isUuid(ctx context.Context, args ...interface{}) (interface{}, error) {
	if val, ok := ensureString(args[0]); ok {
		return rUUID.MatchString(val), nil
	}
	return false, nil
}
