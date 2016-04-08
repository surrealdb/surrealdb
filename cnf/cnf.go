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
	Store string // The backend KV store to use

	DB struct {
		Host string // Surreal host to connect to
		Port string // Surreal port to connect to
		Base string // Base key to use in KV stores
	}

	Cert struct {
		CA struct {
			File string // File location of CA certificate
			Data string // PEM encoded content of certificate
		}
		Crt struct {
			File string // File location of server certificate
			Data string // PEM encoded content of certificate
		}
		Key struct {
			File string // File location of server certificate
			Data string // PEM encoded content of certificate
		}
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

	Port struct {
		Raft int // Raft port
		Http int // Http port
	}

	Conn struct {
		Raft string // Raft port
		Http string // Http port
	}

	Cluster struct {
		Join string   // Comma separated peers to join
		Peer []string // Slice of peers to join
	}

	Logging struct {
		Level    string // Stores the configured logging level
		Output   string // Stores the configured logging output
		Format   string // Stores the configured logging format
		Newrelic string // Stores the configured newrelic license key
	}
}
