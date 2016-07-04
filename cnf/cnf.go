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

package cnf

// Options defines global configuration options
type Options struct {
	DB struct {
		Path string // Path to store the data file
		Host string // Surreal host to connect to
		Port string // Surreal port to connect to
		Base string // Base key to use in KV stores
	}

	Port struct {
		Web int // Web port as an number
		Tcp int // Tcp port as an number
	}

	Conn struct {
		Web string // Web port as a string
		Tcp string // Tcp port as a string
	}

	Cert struct {
		Crt string // File location of server crt
		Key string // File location of server key
		Pem string // File location of server pem
	}

	Auth struct {
		Auth  string // Master authentication username:password
		User  string // Master authentication username
		Pass  string // Master authentication password
		Token string
	}

	Node struct {
		Host string   // Node hostname
		Name string   // Name of this node
		Uniq string   // Uniq of this node
		UUID string   // UUID of this node
		Attr string   // Comma separated tags for this node
		Tags []string // Slice of tags for this node
	}

	Cluster struct {
		Join string   // Comma separated peers to join
		Peer []string // Slice of peers to join
	}

	Backups struct {
		Time string
		Path string
	}

	Logging struct {
		Level    string // Stores the configured logging level
		Output   string // Stores the configured logging output
		Format   string // Stores the configured logging format
		Newrelic string // Stores the configured newrelic license key
	}
}
