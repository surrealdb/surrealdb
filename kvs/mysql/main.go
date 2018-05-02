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
	"time"

	"strings"

	"crypto/tls"
	"crypto/x509"

	"database/sql"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"

	"github.com/go-sql-driver/mysql"
)

func init() {

	kvs.Register("mysql", func(opts *cnf.Options) (db kvs.DB, err error) {

		var pntr *sql.DB

		path := strings.TrimPrefix(opts.DB.Path, "mysql://")

		if cnf.Settings.DB.Cert.SSL {

			cas := x509.NewCertPool()
			all := make([]tls.Certificate, 0, 1)
			car := []byte(cnf.Settings.DB.Cert.CA)
			crt := []byte(cnf.Settings.DB.Cert.Crt)
			key := []byte(cnf.Settings.DB.Cert.Key)

			if ok := cas.AppendCertsFromPEM(car); !ok {
				log.WithPrefix("kvs").Errorln("Failed to append CA file.")
			}

			par, err := tls.X509KeyPair(crt, key)
			if err != nil {
				log.WithPrefix("kvs").Errorln(err)
			}

			mysql.RegisterTLSConfig("default", &tls.Config{
				InsecureSkipVerify: true,
				RootCAs:            cas,
				Certificates:       append(all, par),
			})

		}

		pntr, err = sql.Open("mysql", path)
		if err != nil {
			log.WithPrefix("kvs").Errorln(err)
			return
		}

		// Set the maximum connection lifetime

		pntr.SetConnMaxLifetime(1 * time.Hour)

		// Output logs to the default logger

		mysql.SetLogger(log.Instance())

		// Set the max number of idle connections

		pntr.SetMaxIdleConns(350)

		// Set the max number of open connections

		pntr.SetMaxOpenConns(350)

		// Return the database pointer

		return &DB{pntr: pntr}, err

	})

}
