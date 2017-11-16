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

package fncs

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
)

func hashMd5(ctx context.Context, args ...interface{}) (string, error) {
	v, _ := ensureString(args[0])
	h := md5.New()
	h.Write([]byte(v))
	s := h.Sum(nil)
	return hex.EncodeToString(s), nil
}

func hashSha1(ctx context.Context, args ...interface{}) (string, error) {
	v, _ := ensureString(args[0])
	h := sha1.New()
	h.Write([]byte(v))
	s := h.Sum(nil)
	return hex.EncodeToString(s), nil
}

func hashSha256(ctx context.Context, args ...interface{}) (string, error) {
	v, _ := ensureString(args[0])
	h := sha256.New()
	h.Write([]byte(v))
	s := h.Sum(nil)
	return hex.EncodeToString(s), nil
}

func hashSha512(ctx context.Context, args ...interface{}) (string, error) {
	v, _ := ensureString(args[0])
	h := sha512.New()
	h.Write([]byte(v))
	s := h.Sum(nil)
	return hex.EncodeToString(s), nil
}
