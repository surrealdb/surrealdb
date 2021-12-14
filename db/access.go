// Copyright © 2016 SurrealDB Ltd.
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

	"github.com/surrealdb/surrealdb/cnf"
)

func (e *executor) access(ctx context.Context, kind cnf.Kind) (err error) {

	if perm(ctx) > kind {
		return new(QueryError)
	}

	if kind > cnf.AuthKV && len(e.ns) == 0 {
		return new(BlankError)
	}

	if kind > cnf.AuthNS && len(e.db) == 0 {
		return new(BlankError)
	}

	return

}
