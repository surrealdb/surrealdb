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

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/db"
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
			return
		}

		if err = tcp.Setup(opts); err != nil {
			return
		}

		if err = web.Setup(opts); err != nil {
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

	startCmd.PersistentFlags().StringVarP(&opts.Auth.Auth, "auth", "a", "", flag("auth"))
	startCmd.PersistentFlags().StringVar(&opts.Auth.User, "auth-user", "", flag("auth-user"))
	startCmd.PersistentFlags().StringVar(&opts.Auth.Pass, "auth-pass", "", flag("auth-pass"))

	startCmd.PersistentFlags().StringVarP(&opts.Auth.Auth, "key", "k", "", flag("key"))

	startCmd.PersistentFlags().StringVar(&opts.Cert.Crt, "cert-crt", "", flag("cert-crt"))
	startCmd.PersistentFlags().StringVar(&opts.Cert.Key, "cert-key", "", flag("cert-key"))
	startCmd.PersistentFlags().StringVar(&opts.Cert.Pem, "cert-pem", "", flag("cert-pem"))

	startCmd.PersistentFlags().StringVar(&opts.DB.Base, "db-base", "", flag("db-base"))
	startCmd.PersistentFlags().StringVar(&opts.DB.Path, "db-path", "", flag("db-path"))

	startCmd.PersistentFlags().IntVar(&opts.Port.Tcp, "port-tcp", 0, flag("port-tcp"))
	startCmd.PersistentFlags().IntVar(&opts.Port.Web, "port-web", 0, flag("port-web"))

	startCmd.PersistentFlags().StringVarP(&opts.Node.Name, "name", "n", "", flag("name"))
	startCmd.PersistentFlags().StringVarP(&opts.Node.Attr, "tags", "t", "", flag("tags"))

	startCmd.PersistentFlags().StringVarP(&opts.Cluster.Join, "join", "j", "", flag("join"))

	startCmd.PersistentFlags().MarkHidden("auth-user")
	startCmd.PersistentFlags().MarkHidden("auth-pass")

}
