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

package mysql

import (
	"fmt"
	"regexp"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"database/sql"
	"github.com/go-sql-driver/mysql"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
)

func init() {
	kvs.Register("mysql", New)
}

func New(opts *cnf.Options) (ds kvs.DS, err error) {

	var db *sql.DB

	opts.DB.Path, err = config(opts)
	if err != nil {
		return
	}

	db, err = sql.Open("mysql", opts.DB.Path)
	if err != nil {
		return
	}

	return &DS{db: db, ck: opts.DB.Key}, err

}

func config(opts *cnf.Options) (path string, err error) {

	re := regexp.MustCompile(`^mysql://` +
		`((?:(?P<user>.*?)(?::(?P<passwd>.*))?@))?` +
		`(?:(?:(?P<addr>[^\/]*))?)?` +
		`\/(?P<dbname>.*?)` +
		`(?:\?(?P<params>[^\?]*))?$`)

	ma := re.FindStringSubmatch(opts.DB.Path)

	if len(ma) == 0 || ma[4] == "" || ma[5] == "" {
		err = fmt.Errorf("Specify a valid data store configuration path. Use the help command for further instructions.")
	}

	if opts.DB.Cert.SSL {
		pool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(opts.DB.Cert.CA)
		if err != nil {
			err = fmt.Errorf("Could not read file %s", opts.DB.Cert.CA)
		}
		if ok := pool.AppendCertsFromPEM(pem); !ok {
			return "", fmt.Errorf("Could not read file %s", opts.DB.Cert.CA)
		}
		cert := make([]tls.Certificate, 0, 1)
		pair, err := tls.LoadX509KeyPair(opts.DB.Cert.Crt, opts.DB.Cert.Key)
		if err != nil {
			return "", err
		}
		cert = append(cert, pair)
		mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs:            pool,
			Certificates:       cert,
			InsecureSkipVerify: true,
		})
	}

	if opts.DB.Cert.SSL {
		path += fmt.Sprintf("%stcp(%s)/%s?tls=custom", ma[1], ma[4], ma[5])
	} else {
		path += fmt.Sprintf("%stcp(%s)/%s", ma[1], ma[4], ma[5])
	}

	return

}
