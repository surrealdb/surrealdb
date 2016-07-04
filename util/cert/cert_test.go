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
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var enc = `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIFFDCCAvygAwIBAgIIFF4HaGPiklwwDQYJKoZIhvcNAQENBQAwMDEOMAwGA1UE
ChMFQWJjdW0xHjAcBgNVBAMTFUNlcnRpZmljYXRlIEF1dGhvcml0eTAeFw0xNjA3
MDQwNzUxMTNaFw0yNjA3MDQwNzUxMTNaMC0xDjAMBgNVBAoTBUFiY3VtMRswGQYD
VQQDExJTZXJ2ZXIgQ2VydGlmaWNhdGUwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAw
ggIKAoICAQC4eHcJGB0gS1MvBJsZXd6R8Ti50tMEBjqbeyv/cPDVpqtzajoEaPGr
21klOg4yushWdK6TxKTXHpTTP/JxbaomKS9lhuODqrglfxf3KVglaqoE7ZPUsihj
sh8bqI4Sy6Vx7OX2kOVZWm/R86se1HvmaPHfvokZqjcS+T0wvO3mozMgBqZU3Z9t
gyqyLbqKH0FpMscBd2gP4D69nzDwnQgb8BF1AG70905c7aGHOTgJqt/GGeIqiqjZ
62ApCDjaySh/oYfThCVxLn/QRXaZ2d6da6hOKOmSa82pvMmG5MnbwHdFIHCwDdyQ
FAQOuLUW1fVSrHDeDeocPg0RwJPRYSsFEHlbQAjW+xFA6E8iei6IzRc+l6ZmI0RM
L4MU1DuAbXSDc5RMtLrhK8mrwEcfrEfVY/w0Pm+LHmKUNenQK0PPNgQ6glqfP4wk
YsWCQ9gaqBZcQOyZwhxOxU9uUVFePIND1+dvhTijPVa+6tEMxqYJPzvlHBkIOnGM
g8AkzKKvW7i8a7FjrQ8AX83I54eN/uTx4xXMyO8q8bRONHqY4XgM0tvpP7NWKNyr
zDKc+zwPWMaFiuy8a+GSST/0HpPLtNa+DuWqtGnHDRBqVKuAVyU1Dm0CR6ZHFxca
w0cq8VhkmkR+jl8cZWe3/WbW2+aaeKVp06WR4r0kZhqrgyEZIHEJ8QIDAQABozUw
MzAOBgNVHQ8BAf8EBAMCAvwwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDAYDVR0TAQH/
BAIwADANBgkqhkiG9w0BAQ0FAAOCAgEATIGvRoQdFrOiyLPMsNC3I49fUu5sfxse
OesCgDfzIXgUxZuHTiSbon6QP3+YfrRvWunuhl68xlQLZk7dBhZ0CM7Qe/TS06vL
Shjp2BsuJs2Ej8Vw0xuqAjCkuSOXqsu3Lt3G6edSrsZNBoi+76/uIOnaoZ19N4n5
R5p6tggwRHf7xzggjvmRx+iyjyn3uY8LkN57xNZEjFxI9MihnKrUx9NofsdfYpJ3
VTQoXN+dIPFO+PFCmZ4/EVd/hxc/kKHxiUXQ9Zs6KFEYkh4mVmt6BT3mO6U4bE+3
aZSKx7JJs0A9USyWUP7p6P7QMCBtY+UxXqY5CKIyi0i/3+REbdmXfIQPeolliiqI
M17g5KCJO8wbBRzyzIsL14KkXj4zaSud9vBcJR6kJ6skOKM629zPyKtTlE56bBEy
Bi3woBxcw9E71Cd9Qbibv34NXU7lagv4a+Mz8pKnRXSbVTEHgPbfqFdnK4+OYDee
7HUqZKacL1+CT2nxo/ll1Bb93SUrSz72dJKZdSDUJ1YiwfhAX0LYdgFYihGFyftf
TKMZpHXH4xE0TVPv6y/jpMEH/6EKbxgYyJUwaJndSEBuCEMEUzxwr0/EXY2shk0R
4aigCFBVyrwOTlzoVJcNx5jEMgBQ8Npvxpm898GxZssDJpk74AICs3iy7OjDcKLn
EPJLOqCsvLs=
-----END CERTIFICATE-----
`

func TestValid(t *testing.T) {

	Convey("PEM encoded file should extract", t, func() {
		err := Extract(enc, "test.crt", "test.key")
		So(err, ShouldBeNil)
		os.Remove("test.crt")
		os.Remove("test.key")
	})

}

func TestInvalid(t *testing.T) {

	Convey("Unknown encoded file should not extract", t, func() {
		err := Extract("err", "test.crt", "test.key")
		So(err, ShouldNotBeNil)
		os.Remove("test.crt")
		os.Remove("test.key")
	})

	Convey("Certificate file can not be created", t, func() {
		err := Extract(enc, "nofolder/test.crt", "test.key")
		So(err, ShouldNotBeNil)
		os.Remove("test.crt")
		os.Remove("test.key")
	})

	Convey("Private key file can not be created", t, func() {
		err := Extract(enc, "test.crt", "nofolder/test.key")
		So(err, ShouldNotBeNil)
		os.Remove("test.crt")
		os.Remove("test.key")
	})

}
