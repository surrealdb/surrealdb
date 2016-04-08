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

type certClientOptions struct {
	CA struct {
		Crt string
		Key string
	}
	Out struct {
		Crt string
		Key string
	}
}

var certClientOpt *certClientOptions

var certClientCmd = &cobra.Command{
	Use:     "client",
	Short:   "Create a new client certificate and key.",
	Example: "  surreal cert client --ca-crt crt/ca.crt --ca-key crt/ca.key --out-crt crt/client.crt --out-key crt/client.key",
	PreRunE: func(cmd *cobra.Command, args []string) error {

		if len(certClientOpt.CA.Crt) == 0 {
			return fmt.Errorf("Please provide a CA certificate file path.")
		}

		if len(certClientOpt.CA.Key) == 0 {
			return fmt.Errorf("Please provide a CA private key file path.")
		}

		if len(certClientOpt.Out.Crt) == 0 {
			return fmt.Errorf("Please provide a certificate file path.")
		}

		if len(certClientOpt.Out.Key) == 0 {
			return fmt.Errorf("Please provide a private key file path.")
		}

		return nil

	},
	RunE: func(cmd *cobra.Command, args []string) error {

		var enc []byte

		caCrtFile, err := ioutil.ReadFile(certClientOpt.CA.Crt)
		if err != nil {
			return fmt.Errorf("Could not read file: %#v", certClientOpt.CA.Crt)
		}

		caCrtData, _ := pem.Decode(caCrtFile)

		caCrt, err := x509.ParseCertificate(caCrtData.Bytes)
		if err != nil {
			return fmt.Errorf("Could not parse CA certificate: %#v", err)
		}

		caKeyFile, err := ioutil.ReadFile(certClientOpt.CA.Key)
		if err != nil {
			return fmt.Errorf("Could not read file: %#v", certClientOpt.CA.Crt)
		}

		caKeyData, _ := pem.Decode(caKeyFile)

		caKey, err := x509.ParsePKCS1PrivateKey(caKeyData.Bytes)
		if err != nil {
			return fmt.Errorf("Could not parse CA private key: %#v", err)
		}

		csr := &x509.Certificate{
			Subject: pkix.Name{
				CommonName:   "Surreal Client Certificate",
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
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}

		key, err := rsa.GenerateKey(rand.Reader, 4096)
		if err != nil {
			return fmt.Errorf("Certificate generation failed: %#v", err)
		}

		prv := x509.MarshalPKCS1PrivateKey(key)

		pub, err := x509.CreateCertificate(rand.Reader, csr, caCrt, &key.PublicKey, caKey)
		if err != nil {
			return fmt.Errorf("Certificate generation failed: %#v", err)
		}

		enc = pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: pub,
		})

		log.Printf("Saving client certificate file into %v...", certClientOpt.Out.Crt)
		if err := ioutil.WriteFile(certClientOpt.Out.Crt, enc, 0644); err != nil {
			return fmt.Errorf("Unable to write certificate file to %v: %#v", certClientOpt.Out.Crt, err)
		}

		enc = pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: prv,
		})

		log.Printf("Saving client private key file into %v...", certClientOpt.Out.Key)
		if err := ioutil.WriteFile(certClientOpt.Out.Key, enc, 0644); err != nil {
			return fmt.Errorf("Unable to write private key file to %v: %#v", certClientOpt.Out.Key, err)
		}

		return nil

	},
}

func init() {

	certClientOpt = &certClientOptions{}

	certClientCmd.PersistentFlags().StringVar(&certClientOpt.CA.Crt, "ca-crt", "", "The path to the CA certificate file.")
	certClientCmd.PersistentFlags().StringVar(&certClientOpt.CA.Key, "ca-key", "", "The path to the CA private key file.")

	certClientCmd.PersistentFlags().StringVar(&certClientOpt.Out.Crt, "out-crt", "", "The path destination for the client certificate file.")
	certClientCmd.PersistentFlags().StringVar(&certClientOpt.Out.Key, "out-key", "", "The path destination for the client private key file.")

}
