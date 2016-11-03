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

package sql

var funcs = map[string]map[int]bool{

	"abs": {
		1: true,
	},

	"avg": {
		1: true,
	},

	"ceil": {
		1: true,
	},

	"count": {
		1: true,
	},

	"date": {
		0: true,
		1: true,
	},

	"day": {
		0: true,
		1: true,
	},

	"derivative": {
		1: true,
	},

	"difference": {
		1: true,
		2: true,
		3: true,
		4: true,
		5: true,
		6: true,
		7: true,
		8: true,
		9: true,
	},

	"distinct": {
		1: true,
	},

	"floor": {
		1: true,
	},

	"hour": {
		0: true,
		1: true,
	},

	"intersect": {
		1: true,
		2: true,
		3: true,
		4: true,
		5: true,
		6: true,
		7: true,
		8: true,
		9: true,
	},

	"max": {
		1: true,
	},

	"mean": {
		1: true,
	},

	"median": {
		1: true,
	},

	"min": {
		1: true,
	},

	"mins": {
		0: true,
		1: true,
	},

	"mode": {
		1: true,
	},

	"month": {
		0: true,
		1: true,
	},

	"now": {
		0: true,
	},

	"percentile": {
		1: true,
	},

	"round": {
		1: true,
	},

	"stddev": {
		1: true,
	},

	"sum": {
		1: true,
	},

	"table": {
		1: true,
	},

	"thing": {
		2: true,
	},

	"union": {
		1: true,
		2: true,
		3: true,
		4: true,
		5: true,
		6: true,
		7: true,
		8: true,
		9: true,
	},

	"unixtime": {
		0: true,
		1: true,
	},

	"uuid": {
		0: true,
	},

	"variance": {
		1: true,
	},

	"year": {
		0: true,
		1: true,
	},
}
