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
	"os"

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/tcp"
	"github.com/abcum/surreal/web"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the database and http server",
	PreRun: func(cmd *cobra.Command, args []string) {

		fmt.Print(logo)

	},
	RunE: func(cmd *cobra.Command, args []string) (err error) {

		if err = db.Setup(opts); err != nil {
			log.Fatal(err)
			return
		}

		if err = tcp.Setup(opts); err != nil {
			log.Fatal(err)
			return
		}

		if err = web.Setup(opts); err != nil {
			log.Fatal(err)
			return
		}

		return

	},
	PostRun: func(cmd *cobra.Command, args []string) {

		tcp.Exit()
		web.Exit()
		db.Exit()

	},
}

func init() {

	host, _ := os.Hostname()

	startCmd.PersistentFlags().StringVarP(&opts.Auth.Auth, "auth", "a", "root:root", "Master database authentication details.")
	startCmd.PersistentFlags().StringVar(&opts.Auth.User, "auth-user", "", "The master username for the database. Use this as an alternative to the --auth flag.")
	startCmd.PersistentFlags().StringVar(&opts.Auth.Pass, "auth-pass", "", "The master password for the database. Use this as an alternative to the --auth flag.")

	startCmd.PersistentFlags().StringVar(&opts.Cert.Crt, "cert-crt", "", "Path to the server certificate. Needed when running in secure mode.")
	startCmd.PersistentFlags().StringVar(&opts.Cert.Key, "cert-key", "", "Path to the server private key. Needed when running in secure mode.")

	startCmd.PersistentFlags().StringVar(&opts.DB.Cert.CA, "db-ca", "", "Path to the CA file used to connect to the remote database.")
	startCmd.PersistentFlags().StringVar(&opts.DB.Cert.Crt, "db-crt", "", "Path to the certificate file used to connect to the remote database.")
	startCmd.PersistentFlags().StringVar(&opts.DB.Cert.Key, "db-key", "", "Path to the private key file used to connect to the remote database.")
	startCmd.PersistentFlags().StringVar(&opts.DB.Path, "db-path", "", flag("db"))
	startCmd.PersistentFlags().StringVar(&opts.DB.Time, "db-sync", "0s", "Something here")

	startCmd.PersistentFlags().StringVarP(&opts.Cluster.Join, "join", "j", "", flag("join"))

	startCmd.PersistentFlags().StringVarP(&opts.DB.Code, "key", "k", "", flag("key"))

	startCmd.PersistentFlags().StringVarP(&opts.Node.Host, "bind", "b", "0.0.0.0", "The hostname or ip address to listen for connections on.")

	startCmd.PersistentFlags().StringVarP(&opts.Node.Name, "name", "n", host, "The name of this node, used for logs and statistics.")

	startCmd.PersistentFlags().IntVar(&opts.Port.Tcp, "port-tcp", 33693, "The port on which to serve the tcp server.")
	startCmd.PersistentFlags().IntVar(&opts.Port.Web, "port-web", 8000, "The port on which to serve the web server.")

}
