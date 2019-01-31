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

package db

import (
	"context"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
)

func (e *executor) executeUse(ctx context.Context, stm *sql.UseStatement) (out []interface{}, err error) {

	// If a NAMESPACE has been defined, then
	// process the permissions, or return an
	// error if we can't specify a namespace.

	if len(stm.NS) > 0 {

		if perm(ctx) == cnf.AuthKV {
			e.ns = stm.NS
		}

		if perm(ctx) == cnf.AuthNS {
			if e.ns != stm.NS {
				err = new(QueryError)
			}
		}

		if perm(ctx) == cnf.AuthDB {
			if e.ns != stm.NS {
				err = new(QueryError)
			}
		}

		if perm(ctx) == cnf.AuthSC {
			if e.ns != stm.NS {
				err = new(QueryError)
			}
		}

		if perm(ctx) == cnf.AuthNO {
			e.ns = stm.NS
		}

	}

	// If a DATABASE has been defined, then
	// process the permissions, or return an
	// error if we can't specify a database.

	if len(stm.DB) > 0 {

		if perm(ctx) == cnf.AuthKV {
			e.db = stm.DB
		}

		if perm(ctx) == cnf.AuthNS {
			e.db = stm.DB
		}

		if perm(ctx) == cnf.AuthDB {
			if e.db != stm.DB {
				err = new(QueryError)
			}
		}

		if perm(ctx) == cnf.AuthSC {
			if e.db != stm.DB {
				err = new(QueryError)
			}
		}

		if perm(ctx) == cnf.AuthNO {
			e.db = stm.DB
		}

	}

	if err != nil {
		e.ns, e.db = "", ""
	}

	return

}
