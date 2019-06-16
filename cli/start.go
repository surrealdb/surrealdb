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
	"github.com/spf13/cobra"

	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/web"
)

var startCmd = &cobra.Command{
	Use:   "start [flags]",
	Short: "Start the database and http server",
	PreRun: func(cmd *cobra.Command, args []string) {

		log.Display(logo)

	},
	RunE: func(cmd *cobra.Command, args []string) (err error) {

		if err = db.Setup(opts); err != nil {
			log.Fatal(err)
			return
		}

		if err = web.Setup(opts); err != nil {
			log.Fatal(err)
			return
		}

		return

	},
	PostRunE: func(cmd *cobra.Command, args []string) (err error) {

		if err = web.Exit(); err != nil {
			log.Fatal(err)
			return
		}

		if err = db.Exit(); err != nil {
			log.Fatal(err)
			return
		}

		return

	},
}

func init() {

	startCmd.PersistentFlags().StringVarP(&opts.Auth.Auth, "auth", "a", "root:root", "Master database authentication details")
	startCmd.PersistentFlags().StringVar(&opts.Auth.User, "auth-user", "", "The master username for the database. Use this as an alternative to the --auth flag")
	startCmd.PersistentFlags().StringVar(&opts.Auth.Pass, "auth-pass", "", "The master password for the database. Use this as an alternative to the --auth flag")
	startCmd.PersistentFlags().StringSliceVar(&opts.Auth.Addr, "auth-addr", []string{"0.0.0.0/0", "0:0:0:0:0:0:0:0/0"}, "The IP address ranges from which master authentication is possible")

	startCmd.PersistentFlags().StringVar(&opts.DB.Path, "path", "", "Database path used for storing data")
	startCmd.PersistentFlags().IntVar(&opts.Port, "port", 8000, "The port on which to serve the web server")
	startCmd.PersistentFlags().StringVarP(&opts.Bind, "bind", "b", "0.0.0.0", "The hostname or ip address to listen for connections on")

	startCmd.PersistentFlags().StringVarP(&opts.DB.Code, "key", "k", "", "Encryption key to use for on-disk encryption")

	startCmd.PersistentFlags().DurationVar(&opts.DB.Proc.Sync, "db-sync", 0, "A time duration to use when syncing data to persistent storage")
	startCmd.PersistentFlags().DurationVar(&opts.DB.Proc.Shrink, "db-shrink", 0, "A time duration to use when shrinking data on persistent storage")

	startCmd.PersistentFlags().StringVar(&opts.DB.Cert.CA, "kvs-ca", "", "Path to the CA file used to connect to the remote database")
	startCmd.PersistentFlags().StringVar(&opts.DB.Cert.Crt, "kvs-crt", "", "Path to the certificate file used to connect to the remote database")
	startCmd.PersistentFlags().StringVar(&opts.DB.Cert.Key, "kvs-key", "", "Path to the private key file used to connect to the remote database")

	startCmd.PersistentFlags().StringVar(&opts.Cert.Crt, "web-crt", "", "Path to the server certificate. Needed when running in secure mode")
	startCmd.PersistentFlags().StringVar(&opts.Cert.Key, "web-key", "", "Path to the server private key. Needed when running in secure mode")

}
