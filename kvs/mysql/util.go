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

package mysql

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
)

var chars = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

func encrypt(key []byte, src []byte) (dst []byte, err error) {

	if key == nil || len(key) == 0 || len(src) == 0 {
		return src, nil
	}

	// Initiate AES
	block, _ := aes.NewCipher(key)

	// Initiate cipher
	cipher, _ := cipher.NewGCM(block)

	// Initiate nonce
	nonce := random(12)

	dst = cipher.Seal(nil, nonce, src, nil)

	dst = append(nonce[:], dst[:]...)

	return

}

func decrypt(key []byte, src []byte) (dst []byte, err error) {

	if key == nil || len(key) == 0 || len(src) == 0 {
		return src, nil
	}

	// Corrupt
	if len(src) < 12 {
		return src, errors.New("Invalid data")
	}

	// Initiate AES
	block, _ := aes.NewCipher(key)

	// Initiate cipher
	cipher, _ := cipher.NewGCM(block)

	return cipher.Open(nil, src[:12], src[12:], nil)

}

func random(l int) []byte {

	if l == 0 {
		return nil
	}

	i := 0
	t := len(chars)
	m := 255 - (256 % t)
	b := make([]byte, l)
	r := make([]byte, l+(l/4))

	for {

		rand.Read(r)

		for _, rb := range r {
			c := int(rb)
			if c > m {
				continue
			}
			b[i] = chars[c%t]
			i++
			if i == l {
				return b
			}
		}

	}

}
