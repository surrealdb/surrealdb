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

import (
	"net"
	"time"
)

var Settings *Options

// Options defines global configuration options
type Options struct {
	DB struct {
		Key  []byte // Data encryption key
		Code string // Data encryption key string
		Path string // Path to store the data file
		Base string // Base key to use in KV stores
		Proc struct {
			Flush  time.Duration // Timeframe for flushing data
			Shrink time.Duration // Timeframe for shrinking data
		}
		Cert struct {
			CA  string
			Crt string
			Key string
			SSL bool
		}
	}

	Port int // Port as an number

	Conn string // Port as a string

	Bind string // Hostname of the node

	Cert struct {
		Crt string // File location of server crt
		Key string // File location of server key
	}

	Auth struct {
		Auth string       // Master authentication user:pass
		User string       // Master authentication username
		Pass string       // Master authentication password
		Addr []string     // Allowed ip ranges for authentication
		Nets []*net.IPNet // Allowed cidr ranges for authentication
	}

	Query struct {
		Timeout time.Duration // Fixed query timeout
	}

	Logging struct {
		Level  string // Stores the configured logging level
		Output string // Stores the configured logging output
		Format string // Stores the configured logging format
	}
}
