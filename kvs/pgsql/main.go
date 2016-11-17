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

package pgsql

import (
	"fmt"
	"regexp"

	"database/sql"
	_ "github.com/lib/pq"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
)

func init() {
	kvs.Register("pgsql", New)
}

func New(opts *cnf.Options) (ds kvs.DS, err error) {

	var db *sql.DB

	opts.DB.Path, err = config(opts)
	if err != nil {
		return
	}

	db, err = sql.Open("postgres", opts.DB.Path)
	if err != nil {
		return
	}

	return &DS{db: db, ck: opts.DB.Key}, err

}

func config(opts *cnf.Options) (path string, err error) {

	re := regexp.MustCompile(`^pgsql://` +
		`((?:(?P<user>.*?)(?::(?P<passwd>.*))?@))?` +
		`(?:(?:(?P<addr>[^\/]*))?)?` +
		`\/(?P<dbname>.*?)` +
		`(?:\?(?P<params>[^\?]*))?$`)

	ma := re.FindStringSubmatch(opts.DB.Path)

	if len(ma) == 0 || ma[4] == "" || ma[5] == "" {
		err = fmt.Errorf("Specify a valid data store configuration path. Use the help command for further instructions.")
	}

	if opts.DB.Cert.SSL {
		path += fmt.Sprintf("postgres://%s%s/%s?sslmode=verify-ca&sslrootcert=%s&sslcert=%s&sslkey=%s", ma[1], ma[4], ma[5], opts.DB.Cert.CA, opts.DB.Cert.Crt, opts.DB.Cert.Key)
	} else {
		path += fmt.Sprintf("postgres://%s%s/%s?sslmode=disable", ma[1], ma[4], ma[5])
	}

	return

}
