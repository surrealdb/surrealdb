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
	"auth":      `Master database authentication details. (Default root:root)`,
	"auth-pass": `The master password for the database. Use this as an alternative to the --auth flag.`,
	"auth-user": `The master username for the database. Use this as an alternative to the --auth flag.`,
	"cert-crt":  `Path to the server certificate. Needed in secure mode.`,
	"cert-key":  `Path to the server private key. Needed in secure mode.`,
	"cert-pem":  `The PEM encoded certificate and private key data. Use this as an alternative to the --cert-crt and --cert-key flags.`,
	"db-base":   `Name of the root database key. (Default surreal)`,
	"db-path":   `Set database file location. (Default surreal.db)`,
	"join":      `A comma-separated list of addresses to use when a new node is joining an existing cluster. For the first node in a cluster, --join should NOT be specified.`,
	"key":       `Encryption key to use for intra-cluster communications, and on-disk encryption. For AES-128 encryption use a 16 bit key, for AES-192 encryption use a 24 bit key, and for AES-256 encryption use a 32 bit key.`,
	"name":      `The name of this node, used for logs and statistics. When not specified this will default to the hostname of the machine.`,
	"port-tcp":  `The port on which to serve the tcp server. (Default 33693)`,
	"port-web":  `The port on which to serve the web server. (Default 8000)`,
	"signature": `Set the secret key used to digitally sign authentication tokens.`,
	"tags":      `An ordered, comma-separated list of node attributes. Tags are arbitrary strings specifying topography or machine capabilities. Topography might include datacenter designation (e.g. "us-west-1a", "us-west-1b", "us-east-1c"). Machine capabilities might include specialized hardware or number of cores (e.g. "gpu", "x16c"). The relative geographic proximity of two nodes is inferred from the common prefix of the attributes list, so topographic attributes should be specified first and in the same order for all nodes.`,
}

var usage = map[string][]string{
	"join": []string{
		"--join 10.0.0.1",
		"--join 10.0.0.1:33693",
		"--join 10.0.0.1:33693,10.0.0.2:33693",
		"--join 89.13.7.33:33693,example.com:33693",
	},
	"key": []string{
		"--key 1hg7dbrma8ghe547",
		"--key 1hg7dbrma8ghe5473kghvie6",
		"--key 1hg7dbrma8ghe5473kghvie64jgi3ph4",
	},
	"tags": []string{
		"--tags us-west-1b",
		"--tags us-west-1b,gpu",
	},
}
