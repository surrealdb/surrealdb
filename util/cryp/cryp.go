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

package cryp

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

func Encrypt(key []byte, src []byte) (dst []byte, err error) {

	// Check key length
	if len(key) != 32 {
		return src, nil
	}

	// Initiate AES256
	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}

	// Initiate cipher
	cipher, err := cipher.NewGCM(block)
	if err != nil {
		return
	}

	// Generate new nonce
	nonce := make([]byte, 12)
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return
	}

	dst = cipher.Seal(nil, nonce, src, nil)

	dst = append(nonce[:], dst[:]...)

	return

}

func Decrypt(key []byte, src []byte) (dst []byte, err error) {

	// Check key length
	if len(key) != 32 {
		return src, nil
	}

	// Initiate AES256
	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}

	// Initiate cipher
	cipher, err := cipher.NewGCM(block)
	if err != nil {
		return
	}

	return cipher.Open(nil, src[:12], src[12:], nil)

}
