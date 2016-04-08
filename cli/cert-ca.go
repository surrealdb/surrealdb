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
	"math/big"
	"time"

	"io/ioutil"

	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/log"
)

type certCaOptions struct {
	Out struct {
		Crt string
		Key string
	}
}

var certCaOpt *certCaOptions

var certCaCmd = &cobra.Command{
	Use:     "ca",
	Short:   "Create a new CA certificate and key.",
	Example: "  surreal cert ca --out-crt crt/ca.crt --out-key crt/ca.key",
	PreRunE: func(cmd *cobra.Command, args []string) error {

		if len(certCaOpt.Out.Crt) == 0 {
			return fmt.Errorf("Please provide a certificate file path.")
		}

		if len(certCaOpt.Out.Key) == 0 {
			return fmt.Errorf("Please provide a private key file path.")
		}

		return nil

	},
	RunE: func(cmd *cobra.Command, args []string) error {

		var enc []byte

		csr := &x509.Certificate{
			IsCA: true,
			Subject: pkix.Name{
				CommonName:   "Surreal CA",
				Organization: []string{"Surreal"},
			},
			Issuer: pkix.Name{
				CommonName:   "Surreal CA",
				Organization: []string{"Surreal"},
			},
			BasicConstraintsValid: true,
			SignatureAlgorithm:    x509.SHA512WithRSA,
			PublicKeyAlgorithm:    x509.ECDSA,
			NotBefore:             time.Now(),
			NotAfter:              time.Now().AddDate(10, 0, 0),
			SerialNumber:          big.NewInt(time.Now().UnixNano()),
			KeyUsage: x509.KeyUsageCertSign |
				x509.KeyUsageDigitalSignature |
				x509.KeyUsageKeyAgreement |
				x509.KeyUsageKeyEncipherment |
				x509.KeyUsageDataEncipherment |
				x509.KeyUsageContentCommitment,
		}

		key, err := rsa.GenerateKey(rand.Reader, 4096)
		if err != nil {
			return fmt.Errorf("CA certificate generation failed: %#v", err)
		}

		prv := x509.MarshalPKCS1PrivateKey(key)

		pub, err := x509.CreateCertificate(rand.Reader, csr, csr, &key.PublicKey, key)
		if err != nil {
			return fmt.Errorf("CA certificate generation failed: %#v", err)
		}

		enc = pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: pub,
		})

		log.Printf("Saving CA certificate file into %v...", certCaOpt.Out.Crt)
		if err := ioutil.WriteFile(certCaOpt.Out.Crt, enc, 0644); err != nil {
			return fmt.Errorf("Unable to write certificate file to %v: %#v", certCaOpt.Out.Crt, err)
		}

		enc = pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: prv,
		})

		log.Printf("Saving CA private key file into %v...", certCaOpt.Out.Key)
		if err := ioutil.WriteFile(certCaOpt.Out.Key, enc, 0644); err != nil {
			return fmt.Errorf("Unable to write private key file to %v: %#v", certCaOpt.Out.Key, err)
		}

		return nil

	},
}

func init() {

	certCaOpt = &certCaOptions{}

	certCaCmd.PersistentFlags().StringVar(&certCaOpt.Out.Crt, "out-crt", "", "The path destination for the CA certificate file.")
	certCaCmd.PersistentFlags().StringVar(&certCaOpt.Out.Key, "out-key", "", "The path destination for the CA private key file.")

}
