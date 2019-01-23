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
	"fmt"
	"time"

	"github.com/abcum/surreal/sql"
)

// OptsError occurs when a query can not be run.
type OptsError struct{}

// Error returns the string representation of the error.
func (e *OptsError) Error() string {
	return fmt.Sprintf("Unable to set database options.")
}

// QueryError occurs when a query can not be run.
type QueryError struct{}

// Error returns the string representation of the error.
func (e *QueryError) Error() string {
	return fmt.Sprintf("Unable to perform live query.")
}

// TimerError occurs when a query times out.
type TimerError struct {
	timer time.Duration
}

// Error returns the string representation of the error.
func (e *TimerError) Error() string {
	return fmt.Sprintf("Query timeout of %v exceeded", e.timer)
}

// TableError occurs when an table value is unable to be written.
type TableError struct {
	table string
}

// Error returns the string representation of the error.
func (e *TableError) Error() string {
	return fmt.Sprintf("Unable to write to the '%v' table while it is setup as a view", e.table)
}

// PermsError occurs when a table query is not allowed.
type PermsError struct {
	table string
}

// Error returns the string representation of the error.
func (e *PermsError) Error() string {
	return fmt.Sprintf("You don't have permission to perform this query on the '%v' table", e.table)
}

// LimitError occurs when a 'limit' expression is invalid.
type LimitError struct {
	found interface{}
}

// Error returns the string representation of the error.
func (e *LimitError) Error() string {
	return fmt.Sprintf("Found '%v' but LIMIT expression must be a number", e.found)
}

// StartError occurs when a 'start' expression is invalid.
type StartError struct {
	found interface{}
}

// Error returns the string representation of the error.
func (e *StartError) Error() string {
	return fmt.Sprintf("Found '%v' but START expression must be a number", e.found)
}

// VersnError occurs when a 'version' expression is invalid.
type VersnError struct {
	found interface{}
}

// Error returns the string representation of the error.
func (e *VersnError) Error() string {
	return fmt.Sprintf("Found '%v' but VERSION expression must be a date or time", e.found)
}

// ExistError occurs when a record already exists.
type ExistError struct {
	exist *sql.Thing
}

// Error returns the string representation of the error.
func (e *ExistError) Error() string {
	return fmt.Sprintf("Database record '%v' already exists", e.exist)
}

// FieldError occurs when a field does not conform to the specified assertion.
type FieldError struct {
	field interface{}
	found interface{}
	check interface{}
}

// Error returns the string representation of the error.
func (e *FieldError) Error() string {
	return fmt.Sprintf("Found '%v' for field '%v' but field must conform to: %s", e.found, e.field, e.check)
}

// IndexError occurs when an index value is unable to be written.
type IndexError struct {
	tb   string
	name *sql.Ident
	cols sql.Idents
	vals []interface{}
}

// Error returns the string representation of the error.
func (e *IndexError) Error() string {
	return fmt.Sprintf("Duplicate entry for [%v] wth values %v in index '%s' on '%s'", e.cols, e.vals, e.name, e.tb)
}
