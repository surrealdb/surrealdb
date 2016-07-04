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

package cert

import (
	"fmt"
	"os"

	"encoding/pem"
)

func Extract(enc, key, crt string) (err error) {

	var file *os.File

	data := []byte(enc)

	pemk, pemc := extract(data)
	if pemk == nil || pemc == nil {
		return fmt.Errorf("Can not decode PEM encoded file")
	}

	file, err = os.Create(key)
	if err != nil {
		return fmt.Errorf("Can not decode PEM encoded private key into %s", key)
	}
	file.Write(pemk)
	file.Close()

	file, err = os.Create(crt)
	if err != nil {
		return fmt.Errorf("Can not decode PEM encoded certificate into %s", crt)
	}
	file.Write(pemc)
	file.Close()

	return

}

func extract(data []byte) (key []byte, crt []byte) {

	key = extractKey(data)
	crt = extractCrt(data)

	return

}

func extractKey(data []byte) (output []byte) {

	var block *pem.Block

	for len(data) > 0 {
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type == "RSA PRIVATE KEY" {
			output = append(output, pem.EncodeToMemory(block)...)
		}
	}

	return

}

func extractCrt(data []byte) (output []byte) {

	var block *pem.Block

	for len(data) > 0 {
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			output = append(output, pem.EncodeToMemory(block)...)
		}
	}

	return

}
