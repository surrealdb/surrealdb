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

package db

import (
	"sync"
)

type cache struct {
	items sync.Map
}

func (c *cache) Clr() {
	c.items.Range(func(key interface{}, _ interface{}) bool {
		c.items.Delete(key)
		return true
	})
}

func (c *cache) Del(key string) {
	c.items.Delete(key)
}

func (c *cache) Has(key string) bool {
	_, ok := c.items.Load(key)
	return ok
}

func (c *cache) Get(key string) interface{} {
	val, _ := c.items.Load(key)
	return val
}

func (c *cache) Put(key string, val interface{}) {
	c.items.Store(key, val)
}
