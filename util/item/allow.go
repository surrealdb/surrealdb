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
	"github.com/robertkrimen/otto"
	"github.com/yuin/gopher-lua"

	"github.com/abcum/surreal/cnf"
)

func (this *Doc) Allow(cond string) (val bool) {

	this.getRules()

	if rule, ok := this.rules[cond]; ok {

		val = (rule.Rule == "ACCEPT")

		if rule.Rule == "CUSTOM" {

			if cnf.Settings.DB.Lang == "js" {

				vm := otto.New()

				vm.Set("doc", this.current.Copy())

				ret, err := vm.Run("(function() { " + rule.Code + " })()")
				if err != nil {
					return false
				}

				if ret.IsDefined() {
					val, _ := ret.ToBoolean()
					return val
				} else {
					return false
				}

			}

			if cnf.Settings.DB.Lang == "lua" {

				vm := lua.NewState()
				defer vm.Close()

				vm.SetGlobal("doc", toLUA(vm, this.current.Copy()))

				if err := vm.DoString(rule.Code); err != nil {
					return false
				}

				ret := vm.Get(-1)

				if lua.LVAsBool(ret) {
					return true
				} else {
					return false
				}

			}

		}

	}

	return

}
