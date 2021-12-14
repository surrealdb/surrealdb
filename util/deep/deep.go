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

package deep

import (
	"time"

	"reflect"
)

// Copy returns a deep copy of the source object.
func Copy(src interface{}) interface{} {

	if src == nil {
		return nil
	}

	prime := reflect.ValueOf(src)

	clone := reflect.New(prime.Type()).Elem()

	copy(prime, clone)

	return clone.Interface()

}

func copy(prime, clone reflect.Value) {

	switch prime.Kind() {

	case reflect.Ptr:
		value := prime.Elem()
		if !value.IsValid() {
			return
		}
		clone.Set(reflect.New(value.Type()))
		copy(value, clone.Elem())

	case reflect.Interface:
		if prime.IsNil() {
			return
		}
		value := prime.Elem()
		alike := reflect.New(value.Type()).Elem()
		copy(value, alike)
		clone.Set(alike)

	case reflect.Struct:
		t, ok := prime.Interface().(time.Time)
		if ok {
			clone.Set(reflect.ValueOf(t))
			return
		}
		for i := 0; i < prime.NumField(); i++ {
			if prime.Type().Field(i).PkgPath != "" {
				continue
			}
			copy(prime.Field(i), clone.Field(i))
		}

	case reflect.Slice:
		clone.Set(reflect.MakeSlice(prime.Type(), prime.Len(), prime.Cap()))
		for i := 0; i < prime.Len(); i++ {
			copy(prime.Index(i), clone.Index(i))
		}

	case reflect.Map:
		clone.Set(reflect.MakeMap(prime.Type()))
		for _, key := range prime.MapKeys() {
			value := prime.MapIndex(key)
			alike := reflect.New(value.Type()).Elem()
			copy(value, alike)
			clone.SetMapIndex(key, alike)
		}

	default:
		clone.Set(prime)

	}

}
