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

	"github.com/abcum/surreal/util/fake"
)

func rand(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.DecimalBetween(0, 1), nil
}

func randBool(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.Bool(), nil
}

func randGuid(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.Guid(), nil
}

func randUuid(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.Uuid(), nil
}

func randEnum(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 0:
		return nil, nil
	default:
		return args[fake.IntegerBetween(0, len(args))], nil
	}
}

func randTime(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 2:
		if b, ok := ensureTime(args[0]); ok {
			if e, ok := ensureTime(args[1]); ok {
				return fake.TimeBetween(b, e), nil
			}
		}
	}
	return fake.Time(), nil
}

func randString(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 1:
		if l, ok := ensureInt(args[0]); ok {
			return fake.StringLength(int(l)), nil
		}
	case 2:
		if b, ok := ensureInt(args[0]); ok {
			if e, ok := ensureInt(args[1]); ok {
				return fake.StringBetween(int(b), int(e)), nil
			}
		}
	}
	return fake.String(), nil
}

func randInteger(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 2:
		if b, ok := ensureInt(args[0]); ok {
			if e, ok := ensureInt(args[1]); ok {
				return float64(fake.IntegerBetween(int(b), int(e))), nil
			}
		}
	}
	return float64(fake.Integer()), nil
}

func randDecimal(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 2:
		if b, ok := ensureFloat(args[0]); ok {
			if e, ok := ensureFloat(args[1]); ok {
				return fake.DecimalBetween(b, e), nil
			}
		}
	}
	return fake.Decimal(), nil
}

func randWord(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.Word(), nil
}

func randSentence(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 2:
		if b, ok := ensureInt(args[0]); ok {
			if e, ok := ensureInt(args[1]); ok {
				return fake.SentenceBetween(int(b), int(e)), nil
			}
		}
	}
	return fake.Sentence(), nil
}

func randParagraph(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 2:
		if b, ok := ensureInt(args[0]); ok {
			if e, ok := ensureInt(args[1]); ok {
				return fake.ParagraphBetween(int(b), int(e)), nil
			}
		}
	}
	return fake.Paragraph(), nil
}

func randPersonEmail(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.PersonEmail(), nil
}

func randPersonPhone(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.PersonPhone(), nil
}

func randPersonFullname(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.PersonFullname(), nil
}

func randPersonFirstname(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.PersonFirstname(), nil
}

func randPersonLastname(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.PersonLastname(), nil
}

func randPersonUsername(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.PersonUsername(), nil
}

func randPersonJobtitle(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.PersonJobtitle(), nil
}

func randCompanyName(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.CompanyName(), nil
}

func randCompanyIndustry(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.CompanyIndustry(), nil
}

func randLocationName(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationName(), nil
}

func randLocationAddress(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationAddress(), nil
}

func randLocationStreet(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationStreet(), nil
}

func randLocationCity(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationCity(), nil
}

func randLocationState(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationState(), nil
}

func randLocationCounty(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationCounty(), nil
}

func randLocationZipcode(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationZipcode(), nil
}

func randLocationPostcode(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationPostcode(), nil
}

func randLocationCountry(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationCountry(), nil
}

func randLocationAltitude(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationAltitude(), nil
}

func randLocationLatitude(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationLatitude(), nil
}

func randLocationLongitude(ctx context.Context, args ...interface{}) (interface{}, error) {
	return fake.LocationLongitude(), nil
}
