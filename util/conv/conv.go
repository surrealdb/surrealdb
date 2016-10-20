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

package conv

import (
	"fmt"
	"strconv"
	"time"

	"github.com/asaskevich/govalidator"
)

func toNumber(str string) (int64, error) {
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		val = 0.0
	}
	return int64(val), err
}

func toDouble(str string) (float64, error) {
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		val = 0.0
	}
	return float64(val), err
}

func toBoolean(str string) (bool, error) {
	val, err := strconv.ParseBool(str)
	if err != nil {
		val = false
	}
	return bool(val), err
}

// --------------------------------------------------

func ConvertToUrl(obj interface{}) (val string, err error) {
	val = fmt.Sprintf("%v", obj)
	if !govalidator.IsURL(val) {
		err = fmt.Errorf("Not a valid url")
	}
	return
}

func ConvertToUuid(obj interface{}) (val string, err error) {
	val = fmt.Sprintf("%v", obj)
	if !govalidator.IsUUID(val) {
		err = fmt.Errorf("Not a valid uuid")
	}
	return
}

func ConvertToEmail(obj interface{}) (val string, err error) {
	val = fmt.Sprintf("%v", obj)
	if !govalidator.IsEmail(val) {
		err = fmt.Errorf("Not a valid email")
	}
	return govalidator.NormalizeEmail(val)
}

func ConvertToPhone(obj interface{}) (val string, err error) {
	val = fmt.Sprintf("%v", obj)
	if !govalidator.Matches(val, `^[\s\d\+\-\(\)]+$`) {
		err = fmt.Errorf("Not a valid phone")
	}
	return
}

func ConvertToColor(obj interface{}) (val string, err error) {
	val = fmt.Sprintf("%v", obj)
	if !govalidator.IsHexcolor(val) && !govalidator.IsRGBcolor(val) {
		err = fmt.Errorf("Not a valid color")
	}
	return
}

func ConvertToArray(obj interface{}) (val []interface{}, err error) {
	if now, ok := obj.([]interface{}); ok {
		val = now
	} else {
		err = fmt.Errorf("Not a valid array")
	}
	return
}

func ConvertToObject(obj interface{}) (val map[string]interface{}, err error) {
	if now, ok := obj.(map[string]interface{}); ok {
		val = now
	} else {
		err = fmt.Errorf("Not a valid object")
	}
	return
}

func ConvertToDomain(obj interface{}) (val string, err error) {
	val = fmt.Sprintf("%v", obj)
	if !govalidator.IsDNSName(val) {
		err = fmt.Errorf("Not a valid domain name")
	}
	return
}

func ConvertToBase64(obj interface{}) (val string, err error) {
	val = fmt.Sprintf("%v", obj)
	if !govalidator.IsBase64(val) {
		err = fmt.Errorf("Not valid base64 data")
	}
	return
}

func ConvertToString(obj interface{}) (val string, err error) {
	switch now := obj.(type) {
	case string:
		return now, err
	case []interface{}:
		return val, fmt.Errorf("Not valid string")
	case map[string]interface{}:
		return val, fmt.Errorf("Not valid string")
	default:
		return fmt.Sprintf("%v", obj), err
	}
}

func ConvertToNumber(obj interface{}) (val int64, err error) {
	switch now := obj.(type) {
	case int64:
		return int64(now), err
	case float64:
		return int64(now), err
	case string:
		return toNumber(now)
	default:
		return toNumber(fmt.Sprintf("%v", obj))
	}
}

func ConvertToDouble(obj interface{}) (val float64, err error) {
	switch now := obj.(type) {
	case int64:
		return float64(now), err
	case float64:
		return float64(now), err
	case string:
		return toDouble(now)
	default:
		return toDouble(fmt.Sprintf("%v", obj))
	}
}

func ConvertToBoolean(obj interface{}) (val bool, err error) {
	switch now := obj.(type) {
	case int64:
		return now > 0, err
	case float64:
		return now > 0, err
	case string:
		return toBoolean(now)
	default:
		return toBoolean(fmt.Sprintf("%v", obj))
	}
}

func ConvertToDatetime(obj interface{}) (val time.Time, err error) {
	if now, ok := obj.(time.Time); ok {
		val = now
	} else {
		err = fmt.Errorf("Not a valid datetime")
	}
	return
}

func ConvertToLatitude(obj interface{}) (val float64, err error) {
	str := fmt.Sprintf("%v", obj)
	if !govalidator.IsLatitude(str) {
		err = fmt.Errorf("Not a valid latitude")
	}
	return govalidator.ToFloat(str)
}

func ConvertToLongitude(obj interface{}) (val float64, err error) {
	str := fmt.Sprintf("%v", obj)
	if !govalidator.IsLongitude(str) {
		err = fmt.Errorf("Not a valid longitude")
	}
	return govalidator.ToFloat(str)
}

func ConvertToOneOf(obj interface{}, pos ...interface{}) (val interface{}, err error) {
	for _, now := range pos {
		if num, ok := obj.(int64); ok {
			if float64(num) == now {
				return obj, err
			}
		} else if obj == now {
			return obj, err
		}
	}
	return nil, fmt.Errorf("Not a valid option")
}
