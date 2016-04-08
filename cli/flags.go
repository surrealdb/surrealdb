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
	"auth":      `Master authentication details, used when connecting to the database. (Default root:root)`,
	"auth-user": `The username to be used for accessing the database when using the http interface. Use this as an alternative to the --auth flag.`,
	"auth-pass": `The password to be used for accessing the database when using the http interface. Use this as an alternative to the --auth flag.`,
	"base":      `Name of the root database key`,
	"db":        `Set backend datastore. (Default cockroachdb://127.0.0.1:26257)`,
	"name":      `The name of this node, used for logs and statistics. When not specified this will default to the hostname of the machine.`,
	"join":      `A comma-separated list of addresses to use when a new node is joining an existing cluster. For the first node in a cluster, --join should NOT be specified.`,
	"tags":      `An ordered, comma-separated list of node attributes. Tags are arbitrary strings specifying topography or machine capabilities. Topography might include datacenter designation (e.g. "us-west-1a", "us-west-1b", "us-east-1c"). Machine capabilities might include specialized hardware or number of cores (e.g. "gpu", "x16c"). The relative geographic proximity of two nodes is inferred from the common prefix of the attributes list, so topographic attributes should be specified first and in the same order for all nodes.`,
	"signature": `Set the secret key used to digitally sign authentication tokens.`,
	"cert-crt":  `Path to the client or server certificate. Needed in secure mode.`,
	"cert-key":  `Path to the client or server private key. Needed in secure mode.`,
	"port-http": `The port on which to serve the http server. (Default 8000)`,
	"port-raft": `The port on which to serve the raft server. (Default 33693)`,
}

var usage = map[string][]string{
	"auth": []string{
		"--auth username:password",
	},
	"join": []string{
		"--join 10.0.0.1",
		"--join 10.0.0.1:33693",
		"--join 10.0.0.1:33693,10.0.0.2:33693",
		"--join 89.13.7.33:33693,example.com:33693",
	},
	"tags": []string{
		"--tags us-west-1b",
		"--tags us-west-1b,gpu",
	},
}
