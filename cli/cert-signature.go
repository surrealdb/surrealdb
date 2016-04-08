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

package cli

import (
	"fmt"

	"io/ioutil"

	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/log"
)

type certSignatureOptions struct {
	Out struct {
		Pub string
		Prv string
	}
}

var certSignatureOpt *certSignatureOptions

var certSignatureCmd = &cobra.Command{
	Use:     "signature",
	Short:   "Create a new authentication token certificate and key.",
	Example: "  surreal cert signature --out-pri crt/signature.key --out-pub crt/signature.pub",
	PreRunE: func(cmd *cobra.Command, args []string) error {

		if len(certSignatureOpt.Out.Pub) == 0 {
			return fmt.Errorf("Please provide a public key file path.")
		}

		if len(certSignatureOpt.Out.Prv) == 0 {
			return fmt.Errorf("Please provide a private key file path.")
		}

		return nil

	},
	RunE: func(cmd *cobra.Command, args []string) error {

		var enc []byte

		key, err := rsa.GenerateKey(rand.Reader, 4096)
		if err != nil {
			return fmt.Errorf("Signature generation failed: %#v", err)
		}

		prv := x509.MarshalPKCS1PrivateKey(key)

		pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		if err != nil {
			return fmt.Errorf("Signature generation failed: %#v", err)
		}

		enc = pem.EncodeToMemory(&pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: pub,
		})

		log.Printf("Saving public key file into %v...", certSignatureOpt.Out.Pub)
		if err := ioutil.WriteFile(certSignatureOpt.Out.Pub, enc, 0644); err != nil {
			return fmt.Errorf("Unable to write public key file to %v: %#v", certSignatureOpt.Out.Pub, err)
		}

		enc = pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: prv,
		})

		log.Printf("Saving private key file into %v...", certSignatureOpt.Out.Prv)
		if err := ioutil.WriteFile(certSignatureOpt.Out.Prv, enc, 0644); err != nil {
			return fmt.Errorf("Unable to write private key file to %v: %#v", certSignatureOpt.Out.Prv, err)
		}

		return nil

	},
}

func init() {

	certSignatureOpt = &certSignatureOptions{}

	certSignatureCmd.PersistentFlags().StringVar(&certSignatureOpt.Out.Pub, "out-pub", "", "The path destination for the public key file.")
	certSignatureCmd.PersistentFlags().StringVar(&certSignatureOpt.Out.Prv, "out-pri", "", "The path destination for the private key file.")

}
