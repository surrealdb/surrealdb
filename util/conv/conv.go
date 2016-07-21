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
	"time"

	"github.com/asaskevich/govalidator"
)

func ConvertToUrl(obj interface{}) (val string, err error) {
	val = govalidator.ToString(obj)
	if !govalidator.IsURL(val) {
		err = fmt.Errorf("Not a valid url")
	}
	return
}

func ConvertToUuid(obj interface{}) (val string, err error) {
	val = govalidator.ToString(obj)
	if !govalidator.IsUUID(val) {
		err = fmt.Errorf("Not a valid uuid")
	}
	return
}

func ConvertToEmail(obj interface{}) (val string, err error) {
	val = govalidator.ToString(obj)
	if !govalidator.IsEmail(val) {
		err = fmt.Errorf("Not a valid email")
	}
	return govalidator.NormalizeEmail(val)
}

func ConvertToPhone(obj interface{}) (val string, err error) {
	val = govalidator.ToString(obj)
	if !govalidator.Matches(val, `^[\s\d\+\-\(\)]+$`) {
		err = fmt.Errorf("Not a valid phone")
	}
	return
}

func ConvertToColor(obj interface{}) (val string, err error) {
	val = govalidator.ToString(obj)
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
	val = govalidator.ToString(obj)
	if !govalidator.IsDNSName(val) {
		err = fmt.Errorf("Not a valid domain name")
	}
	return
}

func ConvertToBase64(obj interface{}) (val string, err error) {
	val = govalidator.ToString(obj)
	if !govalidator.IsBase64(val) {
		err = fmt.Errorf("Not valid base64 data")
	}
	return
}

func ConvertToString(obj interface{}) (val string, err error) {
	if now, ok := obj.(string); ok {
		return now, err
	}
	return govalidator.ToString(obj), err
}

func ConvertToNumber(obj interface{}) (val float64, err error) {
	if now, ok := obj.(float64); ok {
		return now, err
	}
	return govalidator.ToFloat(govalidator.ToString(obj))
}

func ConvertToBoolean(obj interface{}) (val bool, err error) {
	if now, ok := obj.(bool); ok {
		return now, err
	}
	return govalidator.ToBoolean(govalidator.ToString(obj))
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
	str := govalidator.ToString(obj)
	if !govalidator.IsLatitude(str) {
		err = fmt.Errorf("Not a valid latitude")
	}
	return govalidator.ToFloat(str)
}

func ConvertToLongitude(obj interface{}) (val float64, err error) {
	str := govalidator.ToString(obj)
	if !govalidator.IsLatitude(str) {
		err = fmt.Errorf("Not a valid longitude")
	}
	return govalidator.ToFloat(str)
}

func ConvertToOneOf(obj interface{}, pos ...interface{}) (val interface{}, err error) {
	for _, now := range pos {
		if num, ok := obj.(int64); ok {
			if float64(num) == now {
				return obj, nil
			}
		} else if obj == now {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("Not a valid option")
}
