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
	"github.com/abcum/surreal/rpc"
	"github.com/abcum/surreal/rpl"
	"github.com/abcum/surreal/web"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the database and http server",
	PreRunE: func(cmd *cobra.Command, args []string) (err error) {

		fmt.Print(logo)

		err = db.Setup(opts)
		if err != nil {
			return
		}

		return

	},
	RunE: func(cmd *cobra.Command, args []string) (err error) {

		err = rpc.Setup(opts)
		if err != nil {
			return
		}

		err = rpl.Setup(opts)
		if err != nil {
			return
		}

		err = web.Setup(opts)
		if err != nil {
			return
		}

		return

	},
	PostRun: func(cmd *cobra.Command, args []string) {

		db.Exit()
		rpc.Exit()
		rpl.Exit()
		web.Exit()

	},
}

func init() {

	startCmd.PersistentFlags().StringVarP(&opts.Auth.Auth, "auth", "a", "", flag("auth"))
	startCmd.PersistentFlags().StringVar(&opts.Auth.User, "auth-user", "", flag("auth-user"))
	startCmd.PersistentFlags().StringVar(&opts.Auth.Pass, "auth-pass", "", flag("auth-pass"))

	startCmd.PersistentFlags().StringVarP(&opts.DB.Base, "base", "", "surreal", flag("base"))

	startCmd.PersistentFlags().StringVarP(&opts.Store, "db", "d", "", flag("db"))

	startCmd.PersistentFlags().IntVar(&opts.Port.Http, "port-http", 0, flag("port-http"))
	startCmd.PersistentFlags().IntVar(&opts.Port.Raft, "port-raft", 0, flag("port-raft"))

	startCmd.PersistentFlags().StringVarP(&opts.Node.Name, "name", "n", "", flag("name"))
	startCmd.PersistentFlags().StringVarP(&opts.Node.Attr, "tags", "t", "", flag("tags"))

	startCmd.PersistentFlags().StringVarP(&opts.Cluster.Join, "join", "j", "", flag("join"))

	startCmd.PersistentFlags().StringVar(&opts.Cert.Crt.File, "cert-crt", "", flag("cert-crt"))
	startCmd.PersistentFlags().StringVar(&opts.Cert.Key.File, "cert-key", "", flag("cert-key"))

	startCmd.PersistentFlags().MarkHidden("auth-user")
	startCmd.PersistentFlags().MarkHidden("auth-pass")
	startCmd.PersistentFlags().MarkHidden("base")

}
