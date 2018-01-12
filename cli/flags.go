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
	"db":     `Database configuration path used for storing data. Available backend stores are memory, file, logr, s3, gcs, or dendrodb. (default "memory").`,
	"key":    `Encryption key to use for intra-cluster communications, and on-disk encryption. For AES-128 encryption use a 16 bit key, for AES-192 encryption use a 24 bit key, and for AES-256 encryption use a 32 bit key.`,
	"size":   `A size in MB which determines the minimum or maximum file size for streaming data file storage. This is used for specifying maximum cached data sizes when using remote streaming storage. (default "5")`,
	"sync":   `A time duration to use when syncing data to persistent storage. To sync data with every write specify '0', otherwise the data will be persisted asynchronously after the specified duration. (default "0s")`,
	"shrink": `A time duration to use when shrinking data on persistent storage. To shrink data asynchronously after a repeating period of time, specify a duration. Disabled by default. (default "0s")`,
	"join":   `A comma-separated list of addresses to use when a new node is joining an existing cluster. For the first node in a cluster, --join should NOT be specified.`,
}

var usage = map[string][]string{
	"db": {
		"--db-path memory",
		"--db-path file://surreal.db",
		"--db-path logr://path/to/surreal.db",
		"--db-path s3://bucket/path/to/surreal.db",
		"--db-path gcs://bucket/path/to/surreal.db",
		"--db-path dendro://user:pass@192.168.1.100",
	},
	"key": {
		"--key 1hg7dbrma8ghe547",
		"--key 1hg7dbrma8ghe5473kghvie6",
		"--key 1hg7dbrma8ghe5473kghvie64jgi3ph4",
	},
	"size": {
		"--db-size 30",
	},
	"sync": {
		"--db-sync 0",
		"--db-sync 5s",
		"--db-sync 1m",
	},
	"shrink": {
		"--db-shrink 30m",
		"--db-shrink 24h",
	},
	"join": {
		"--join 10.0.0.1",
		"--join 10.0.0.1:33693",
		"--join 10.0.0.1:33693,10.0.0.2:33693",
		"--join 89.13.7.33:33693,example.com:33693",
	},
}
