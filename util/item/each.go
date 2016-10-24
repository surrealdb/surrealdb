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
)

func (this *Doc) each(fld *sql.DefineFieldStatement) (err error) {

	return this.current.Walk(func(key string, val interface{}) error {

		old := this.initial.Get(key).Data()

		if fld.Readonly && old != nil {
			this.current.Set(old, key)
			return nil
		}

		if fld.Code != "" {

			if cnf.Settings.DB.Lang == "js" {

				vm := otto.New()

				vm.Set("doc", this.current.Copy())

				ret, err := vm.Run("(function() { " + fld.Code + " })()")
				if err != nil {
					return fmt.Errorf("Problem executing code: %v %v", fld.Code, err.Error())
				}

				if ret.IsUndefined() {
					this.current.Del(key)
				} else {
					val, _ := ret.Export()
					this.current.Set(val, key)
				}

			}

			if cnf.Settings.DB.Lang == "lua" {

				vm := lua.NewState()
				defer vm.Close()

				vm.SetGlobal("doc", toLUA(vm, this.current.Copy()))

				if err := vm.DoString(fld.Code); err != nil {
					return fmt.Errorf("Problem executing code: %v %v", fld.Code, err.Error())
				}

				ret := vm.Get(-1)

				if ret == lua.LNil {
					this.current.Del(key)
				} else {
					this.current.Set(frLUA(ret), key)
				}

			}

		}

		// Ensure that any defined fields are correctly
		// formatted according to their defined type.

		if err = this.chck(fld, key, old, val); err != nil {
			return err
		}

		// Ensure that any default fields are correctly
		// formatted according to their defined type.

		if err = this.chck(fld, key, old, val); err != nil {
			return err
		}

		// Otherwise this is all good.

		return nil

	}, fld.Name)

}

func (this *Doc) chck(fld *sql.DefineFieldStatement, key string, old, val interface{}) (err error) {

	var exi bool

	// Ensure that any fields which have been set to
	// null are reset back to default if specified.

	exi = this.current.Exists(key)
	val = this.current.Get(key).Data()

	if fld.Default != nil && (exi == false || val == nil && fld.Notnull) {
		switch def := fld.Default.(type) {
		case sql.Null, *sql.Null:
			this.current.Set(nil, key)
		default:
			this.current.Set(fld.Default, key)
		case sql.Ident:
			this.current.Set(this.current.Get(def.ID).Data(), key)
		case *sql.Ident:
			this.current.Set(this.current.Get(def.ID).Data(), key)
		}
	}

	// Check to see if any field which has been set
	// to defaults now satisfies the field constraints.

	exi = this.current.Exists(key)
	val = this.current.Get(key).Data()

	if fld.Notnull && exi == true && val == nil {
		return fmt.Errorf("Field '%v' can't be null", key)
	}

	if fld.Mandatory && exi == false {
		return fmt.Errorf("Need to set field '%v'", key)
	}

	// Ensure that any defined fields are correctly
	// formatted according to their defined type.

	if val != nil {

		switch fld.Type {

		case "url":
			if cnv, err := conv.ConvertToUrl(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a URL, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "uuid":
			if cnv, err := conv.ConvertToUuid(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a UUID, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "color":
			if cnv, err := conv.ConvertToColor(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a HEX or RGB color, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "email":
			if cnv, err := conv.ConvertToEmail(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be an email address, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "phone":
			if cnv, err := conv.ConvertToPhone(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a phone number, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "array":
			if cnv, err := conv.ConvertToArray(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be an array, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "object":
			if cnv, err := conv.ConvertToObject(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be an object, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "domain":
			if cnv, err := conv.ConvertToDomain(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a domain name, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "base64":
			if cnv, err := conv.ConvertToBase64(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be base64 data, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "string":
			if cnv, err := conv.ConvertToString(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a string, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "number":
			if cnv, err := conv.ConvertToNumber(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a number, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "double":
			if cnv, err := conv.ConvertToDouble(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a double, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "boolean":
			if cnv, err := conv.ConvertToBoolean(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a boolean, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "datetime":
			if cnv, err := conv.ConvertToDatetime(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a datetime, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "latitude":
			if cnv, err := conv.ConvertToLatitude(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a latitude value, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "longitude":
			if cnv, err := conv.ConvertToLongitude(val); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be a longitude value, but found '%v'", key, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		case "custom":

			if cnv, err := conv.ConvertToOneOf(val, fld.Enum...); err == nil {
				this.current.Set(cnv, key)
			} else {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to be one of %v, but found '%v'", key, fld.Enum, val)
				} else {
					this.current.Iff(old, key)
				}
			}

		}

	}

	if fld.Match != "" {

		if reg, err := regexp.Compile(fld.Match); err != nil {
			return fmt.Errorf("Regular expression /%v/ is invalid", fld.Match)
		} else {
			if !reg.MatchString(fmt.Sprintf("%v", val)) {
				if fld.Validate {
					return fmt.Errorf("Field '%v' needs to match the regular expression /%v/", key, fld.Match)
				} else {
					this.current.Iff(old, key)
				}
			}
		}

	}

	if fld.Min != 0 {

		if val = this.current.Get(key).Data(); val != nil {

			switch now := val.(type) {

			case []interface{}:
				if len(now) < int(fld.Min) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have at least %v items", key, fld.Min)
					} else {
						this.current.Iff(old, key)
					}
				}

			case string:
				if len(now) < int(fld.Min) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have at least %v characters", key, fld.Min)
					} else {
						this.current.Iff(old, key)
					}
				}

			case int64:
				if now < int64(fld.Min) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to be >= %v", key, fld.Min)
					} else {
						this.current.Iff(old, key)
					}
				}

			case float64:
				if now < float64(fld.Min) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to be >= %v", key, fld.Min)
					} else {
						this.current.Iff(old, key)
					}
				}

			}

		}

	}

	if fld.Max != 0 {

		if val = this.current.Get(key).Data(); val != nil {

			switch now := val.(type) {

			case []interface{}:
				if len(now) > int(fld.Max) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have %v or fewer items", key, fld.Max)
					} else {
						this.current.Iff(old, key)
					}
				}

			case string:
				if len(now) > int(fld.Max) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to have %v or fewer characters", key, fld.Max)
					} else {
						this.current.Iff(old, key)
					}
				}

			case int64:
				if now > int64(fld.Max) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to be <= %v", key, fld.Max)
					} else {
						this.current.Iff(old, key)
					}
				}

			case float64:
				if now > float64(fld.Max) {
					if fld.Validate {
						return fmt.Errorf("Field '%v' needs to be <= %v", key, fld.Max)
					} else {
						this.current.Iff(old, key)
					}
				}

			}

		}

	}

	return

}
