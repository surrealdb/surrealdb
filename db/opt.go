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
	"strings"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
)

type options struct {
	fields bool
	events bool
	tables bool
}

func newOptions() *options {
	return &options{
		fields: true,
		events: true,
		tables: true,
	}
}

func (e *executor) executeOpt(ctx context.Context, stm *sql.OptStatement) (out []interface{}, err error) {

	if perm(ctx) >= cnf.AuthSC {
		return nil, new(QueryError)
	}

	switch strings.ToUpper(stm.Name) {
	case "FIELD_QUERIES":
		e.opts.fields = stm.What
	case "EVENT_QUERIES":
		e.opts.events = stm.What
	case "TABLE_QUERIES":
		e.opts.tables = stm.What
	case "IMPORT":
		switch stm.What {
		case true:
			e.opts.fields = false // Set field queries
			e.opts.events = false // Set event queries
			e.opts.tables = true  // Set table queries
		case false:
			e.opts.fields = true // Set field queries
			e.opts.events = true // Set event queries
			e.opts.tables = true // Set table queries
		}
	}

	return

}
