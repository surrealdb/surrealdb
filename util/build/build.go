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

package build

import (
	"runtime"
)

var (
	ver  = "0.1.0" // Version number
	rev  string    // Git revision of this build
	time string    // Build time in UTC (year/month/day hour:min:sec)
)

// Info holds information about the current build
type Info struct {
	Go   string `json:"go"`
	Ver  string `json:"ver"`
	Rev  string `json:"rev"`
	Time string `json:"time"`
}

// GetInfo returns information about the current build
func GetInfo() Info {
	return Info{
		Go:   runtime.Version(),
		Ver:  ver,
		Rev:  rev,
		Time: time,
	}
}
