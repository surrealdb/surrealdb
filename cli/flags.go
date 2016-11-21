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

package cli

var flags = map[string]string{
	"db":   `Database configuration path used for storing data. Available backend stores are boltdb, mysql, or pgsql. (default "boltdb://surreal.db").`,
	"key":  `Encryption key to use for intra-cluster communications, and on-disk encryption. For AES-128 encryption use a 16 bit key, for AES-192 encryption use a 24 bit key, and for AES-256 encryption use a 32 bit key.`,
	"join": `A comma-separated list of addresses to use when a new node is joining an existing cluster. For the first node in a cluster, --join should NOT be specified.`,
	"zone": `The continent that the server is located within. Possible values are: GL (Global), EU (Europe), AS (Asia), NA (North America), SA (South America), OC (Oceania), AF (Africa). (default "GL")`,
}

var usage = map[string][]string{
	"db": []string{
		"--db-path boltdb://surreal.db",
		"--db-path mysql://user:pass@127.0.0.1:3306/database",
		"--db-path pgsql://user:pass@127.0.0.1:5432/database",
	},
	"join": []string{
		"--join 10.0.0.1",
		"--join 10.0.0.1:33693",
		"--join 10.0.0.1:33693,10.0.0.2:33693",
		"--join 89.13.7.33:33693,example.com:33693",
	},
	"key": []string{
		"--enc 1hg7dbrma8ghe547",
		"--enc 1hg7dbrma8ghe5473kghvie6",
		"--enc 1hg7dbrma8ghe5473kghvie64jgi3ph4",
	},
}
