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
	"errors"
	"runtime"
)

type method int8

const (
	_SELECT method = iota
	_CREATE
	_UPDATE
	_DELETE
)

type modify int8

const (
	_REMOVE modify = iota
	_CHANGE
)

const (
	docKeyId  = "id"
	docKeyOne = "0"
	docKeyAll = "*"
)

const (
	logKeyId    = "id"
	logKeyNS    = "ns"
	logKeyDB    = "db"
	logKeySql   = "sql"
	logKeyExe   = "exe"
	logKeyKind  = "kind"
	logKeyVars  = "vars"
	logKeyTime  = "time"
	logKeySpan  = "span"
	logKeyTrace = "trace"
	logKeyError = "error"
	logKeyStack = "stack"
	logKeyFibre = "fibre"
)

const (
	ctxKeyId      = "id"
	ctxKeyDive    = "dive"
	ctxKeyVars    = "vars"
	ctxKeySubs    = "subs"
	ctxKeySpec    = "spec"
	ctxKeyAuth    = "auth"
	ctxKeyKind    = "kind"
	ctxKeyForce   = "force"
	ctxKeyFibre   = "fibre"
	ctxKeyVersion = "version"
)

const (
	varKeyId      = "id"
	varKeyIp      = "ip"
	varKeyEnv     = "ENV"
	varKeyAuth    = "auth"
	varKeyUniq    = "uniq"
	varKeyThis    = "this"
	varKeyScope   = "scope"
	varKeyValue   = "value"
	varKeyAfter   = "after"
	varKeyBefore  = "before"
	varKeyMethod  = "method"
	varKeyOrigin  = "origin"
	varKeyParent  = "parent"
	varKeyParents = "parents"
	varKeySession = "session"
)

var (
	// workerCount specifies how many workers should be used
	// to process each query statement concurrently.
	workerCount = runtime.NumCPU()

	// maxRecursiveQueries specifies how many queries will be
	// processed recursively before the query is cancelled.
	maxRecursiveQueries = uint32(16)

	// queryIdentFailed occurs when a permission query asks
	// for a field, meaning a document has to be fetched.
	queryIdentFailed = errors.New("Found ident but no doc available")

	// errQueryNotExecuted occurs when a transaction has
	// failed, and the following queries are not executed.
	errQueryNotExecuted = errors.New("Query not executed")

	// errRaceCondition occurs when a record which is locked
	// for editing, is updated from within a subquery.
	errRaceCondition = errors.New("Failed to update the same document recursively")

	// errRecursiveOverload occurs when too many subqueries
	// are executed within one other, causing an endless loop.
	errRecursiveOverload = errors.New("Infinite loop when running recursive subqueries")

	// errFeatureNotImplemented occurs when a feature which
	// has not yet been implemented, has been used in a query.
	errFeatureNotImplemented = errors.New("Feature is not yet implemented")

	// paramSearchKeys specifies the order in which context
	// variables should be checked for any specified value.
	paramSearchKeys = []string{ctxKeySpec, ctxKeySubs, ctxKeyVars}
)
