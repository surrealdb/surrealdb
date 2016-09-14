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

package item

import (
	"fmt"
	"regexp"

	"github.com/robertkrimen/otto"
	"github.com/yuin/gopher-lua"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/conv"
	"github.com/abcum/surreal/util/data"
)

func each(fld *sql.DefineFieldStatement, initial *data.Doc, current *data.Doc) (err error) {

	var e bool
	var i interface{}
	var c interface{}

	i = initial.Get(fld.Name).Data()

	if fld.Readonly && i != nil {
		current.Set(i, fld.Name)
		return
	}

	if fld.Code != "" {

		if cnf.Settings.DB.Lang == "js" {

			vm := otto.New()

			vm.Set("doc", current.Copy())

			ret, err := vm.Run("(function() { " + fld.Code + " })()")
			if err != nil {
				return fmt.Errorf("Problem executing code: %v %v", fld.Code, err.Error())
			}

			if ret.IsUndefined() {
				current.Del(fld.Name)
			} else {
				val, _ := ret.Export()
				current.Set(val, fld.Name)
			}

		}

		if cnf.Settings.DB.Lang == "lua" {

			vm := lua.NewState()
			defer vm.Close()

			vm.SetGlobal("doc", toLUA(vm, current.Copy()))

			if err := vm.DoString(fld.Code); err != nil {
				return fmt.Errorf("Problem executing code: %v %v", fld.Code, err.Error())
			}

			ret := vm.Get(-1)

			if ret == lua.LNil {
				current.Del(fld.Name)
			} else {
				current.Set(frLUA(ret), fld.Name)
			}

		}

	}

	c = current.Get(fld.Name).Data()
	e = current.Exists(fld.Name)

	if fld.Default != nil && e == false {
		switch val := fld.Default.(type) {
		case sql.Null, *sql.Null:
			current.Set(nil, fld.Name)
		default:
			current.Set(fld.Default, fld.Name)
		case sql.Ident:
			current.Set(current.Get(val.ID).Data(), fld.Name)
		case *sql.Ident:
			current.Set(current.Get(val.ID).Data(), fld.Name)
		}
	}

	c = current.Get(fld.Name).Data()
	e = current.Exists(fld.Name)

	if fld.Notnull && e == true && c == nil {
		return fmt.Errorf("Field '%v' can't be null", fld.Name)
	}

	if fld.Mandatory && e == false {
		return fmt.Errorf("Need to set field '%v'", fld.Name)
	}

	if c != nil && fld.Type != "" {

		switch fld.Type {

		case "url":
			if val, err := conv.ConvertToUrl(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a URL", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "uuid":
			if val, err := conv.ConvertToUuid(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a UUID", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "color":
			if val, err := conv.ConvertToColor(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a HEX or RGB color", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "email":
			if val, err := conv.ConvertToEmail(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be an email address", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "phone":
			if val, err := conv.ConvertToPhone(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a phone number", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "array":
			if val, err := conv.ConvertToArray(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be an array", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "object":
			if val, err := conv.ConvertToObject(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be an object", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "domain":
			if val, err := conv.ConvertToDomain(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a domain name", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "base64":
			if val, err := conv.ConvertToBase64(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be base64 data", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "string":
			if val, err := conv.ConvertToString(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a string", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "number":
			if val, err := conv.ConvertToNumber(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a number", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "boolean":
			if val, err := conv.ConvertToBoolean(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a boolean", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "datetime":
			if val, err := conv.ConvertToDatetime(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a datetime", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "latitude":
			if val, err := conv.ConvertToLatitude(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a latitude value", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "longitude":
			if val, err := conv.ConvertToLongitude(c); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a longitude value", fld.Name)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		case "custom":

			if val, err := conv.ConvertToOneOf(c, fld.Enum...); err == nil {
				current.Set(val, fld.Name)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be one of %v", fld.Name, fld.Enum)
				} else {
					current.Iff(i, fld.Name)
				}
			}

		}

	}

	if fld.Match != "" {

		if reg, err := regexp.Compile(fld.Match); err != nil {
			return fmt.Errorf("Regular expression /%v/ is invalid", fld.Match)
		} else {
			if !reg.MatchString(fmt.Sprintf("%v", c)) {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to match the regular expression /%v/", fld.Name, fld.Match)
				} else {
					current.Iff(i, fld.Name)
				}
			}
		}

	}

	if fld.Min != 0 {

		if c = current.Get(fld.Name).Data(); c != nil {

			switch now := c.(type) {

			case []interface{}:
				if len(now) < int(fld.Min) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have at least %v items", fld.Name, fld.Min)
					} else {
						current.Iff(i, fld.Name)
					}
				}

			case string:
				if len(now) < int(fld.Min) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have at least %v characters", fld.Name, fld.Min)
					} else {
						current.Iff(i, fld.Name)
					}
				}

			case float64:
				if now < fld.Min {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to be >= %v", fld.Name, fld.Min)
					} else {
						current.Iff(i, fld.Name)
					}
				}

			}

		}

	}

	if fld.Max != 0 {

		if c = current.Get(fld.Name).Data(); c != nil {

			switch now := c.(type) {

			case []interface{}:
				if len(now) > int(fld.Max) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have %v or fewer items", fld.Name, fld.Max)
					} else {
						current.Iff(i, fld.Name)
					}
				}

			case string:
				if len(now) > int(fld.Max) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have %v or fewer characters", fld.Name, fld.Max)
					} else {
						current.Iff(i, fld.Name)
					}
				}

			case float64:
				if now > fld.Max {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to be <= %v", fld.Name, fld.Max)
					} else {
						current.Iff(i, fld.Name)
					}
				}

			}

		}

	}

	c = current.Get(fld.Name).Data()
	e = current.Exists(fld.Name)

	if fld.Default != nil && e == false {
		switch val := fld.Default.(type) {
		case sql.Null, *sql.Null:
			current.Set(nil, fld.Name)
		default:
			current.Set(fld.Default, fld.Name)
		case sql.Ident:
			current.Set(current.Get(val.ID).Data(), fld.Name)
		case *sql.Ident:
			current.Set(current.Get(val.ID).Data(), fld.Name)
		}
	}

	c = current.Get(fld.Name).Data()
	e = current.Exists(fld.Name)

	if fld.Notnull && e == true && c == nil {
		return fmt.Errorf("Field '%v' can't be null", fld.Name)
	}

	if fld.Mandatory && e == false {
		return fmt.Errorf("Need to set field '%v'", fld.Name)
	}

	return

}
