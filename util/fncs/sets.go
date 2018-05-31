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
)

func difference(ctx context.Context, args ...interface{}) ([]interface{}, error) {

	d := make([]interface{}, 0)
	c := make(map[interface{}]int)

	for _, x := range args {
		a, _ := ensureSlice(x)
		for _, v := range a {
			c[v] += 1
		}
	}

	for k, b := range c {
		if b == 1 {
			d = append(d, k)
		}
	}

	return d, nil

}

func distinct(ctx context.Context, args ...interface{}) ([]interface{}, error) {

	d := make([]interface{}, 0)
	c := make(map[interface{}]bool)

	for _, x := range args {
		a, _ := ensureSlice(x)
		for _, v := range a {
			switch v := v.(type) {
			case []interface{}:
				for _, v := range v {
					c[v] = true
				}
			default:
				c[v] = true
			}
		}
	}

	for k := range c {
		d = append(d, k)
	}

	return d, nil

}

func intersect(ctx context.Context, args ...interface{}) ([]interface{}, error) {

	l := len(args)
	d := make([]interface{}, 0)
	c := make(map[interface{}]int)

	for _, x := range args {
		a, _ := ensureSlice(x)
		for _, v := range a {
			c[v] += 1
		}
	}

	for k, b := range c {
		if b == l {
			d = append(d, k)
		}
	}

	return d, nil

}

func union(ctx context.Context, args ...interface{}) ([]interface{}, error) {

	d := make([]interface{}, 0)
	c := make(map[interface{}]bool)

	for _, x := range args {
		a, _ := ensureSlice(x)
		for _, v := range a {
			c[v] = true
		}
	}

	for k := range c {
		d = append(d, k)
	}

	return d, nil

}
