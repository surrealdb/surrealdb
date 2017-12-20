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

type method int

const (
	_SELECT method = iota
	_CREATE
	_UPDATE
	_DELETE
)

const (
	docKeyId  = "id"
	docKeyOne = "0"
	docKeyAll = "*"
)

const (
	ctxKeyId    = "id"
	ctxKeyNs    = "ns"
	ctxKeyDb    = "db"
	ctxKeyVars  = "vars"
	ctxKeySubs  = "subs"
	ctxKeySpec  = "spec"
	ctxKeyAuth  = "auth"
	ctxKeyKind  = "kind"
	ctxKeyScope = "scope"
)

const (
	varKeyId     = "id"
	varKeyIp     = "ip"
	varKeyAuth   = "auth"
	varKeyThis   = "this"
	varKeyScope  = "scope"
	varKeyValue  = "value"
	varKeyAfter  = "after"
	varKeyBefore = "before"
	varKeyMethod = "method"
	varKeyParent = "parent"
	varKeyOrigin = "origin"
)

var (
	workerCount           = runtime.NumCPU() * 2
	queryNotExecuted      = errors.New("Query not executed")
	queryIdentFailed      = errors.New("Found ident but no doc available")
	featureNotImplemented = errors.New("Feature is not yet implemented")
	paramSearchKeys       = []string{ctxKeySpec, ctxKeySubs, ctxKeyVars}
)
