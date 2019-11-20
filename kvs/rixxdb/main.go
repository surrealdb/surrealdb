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

package rixxdb

import (
	"github.com/abcum/rixxdb"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
)

func init() {

	kvs.Register("rixxdb", func(opts *cnf.Options) (db kvs.DB, err error) {

		var pntr *rixxdb.DB

		pntr, err = rixxdb.Open(opts.DB.Path, &rixxdb.Config{
			// Explicitly sync writes
			SyncWrites: true,
			// Set the encryption key
			EncryptionKey: opts.DB.Key,
			// Set the sync offset duration
			FlushPolicy: opts.DB.Proc.Flush,
			// Set the shrink offset duration
			ShrinkPolicy: opts.DB.Proc.Shrink,
		})

		if err != nil {
			log.WithPrefix("kvs").Errorln(err)
			return
		}

		return &DB{pntr: pntr}, err

	})

}
