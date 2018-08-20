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

package kvs

import "context"

// TX represents a database transaction
type TX interface {
	Closed() bool
	Cancel() error
	Commit() error

	All(context.Context, []byte) ([]KV, error)
	AllP(context.Context, []byte, uint64) ([]KV, error)
	AllR(context.Context, []byte, []byte, uint64) ([]KV, error)

	Clr(context.Context, []byte) (KV, error)
	ClrP(context.Context, []byte, uint64) ([]KV, error)
	ClrR(context.Context, []byte, []byte, uint64) ([]KV, error)

	Get(context.Context, int64, []byte) (KV, error)
	GetP(context.Context, int64, []byte, uint64) ([]KV, error)
	GetR(context.Context, int64, []byte, []byte, uint64) ([]KV, error)

	Del(context.Context, int64, []byte) (KV, error)
	DelC(context.Context, int64, []byte, []byte) (KV, error)
	DelP(context.Context, int64, []byte, uint64) ([]KV, error)
	DelR(context.Context, int64, []byte, []byte, uint64) ([]KV, error)

	Put(context.Context, int64, []byte, []byte) (KV, error)
	PutC(context.Context, int64, []byte, []byte, []byte) (KV, error)
}
