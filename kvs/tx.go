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

// TX represents a database transaction
type TX interface {
	Closed() bool
	Cancel() error
	Commit() error

	Clr([]byte) (KV, error)
	ClrP([]byte, uint64) ([]KV, error)
	ClrR([]byte, []byte, uint64) ([]KV, error)

	Get(int64, []byte) (KV, error)
	GetP(int64, []byte, uint64) ([]KV, error)
	GetR(int64, []byte, []byte, uint64) ([]KV, error)

	Del(int64, []byte) (KV, error)
	DelC(int64, []byte, []byte) (KV, error)
	DelP(int64, []byte, uint64) ([]KV, error)
	DelR(int64, []byte, []byte, uint64) ([]KV, error)

	Put(int64, []byte, []byte) (KV, error)
	PutC(int64, []byte, []byte, []byte) (KV, error)
}
