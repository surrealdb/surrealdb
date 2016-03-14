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
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/db"
	"github.com/abcum/surreal/server"
)

var opts *cnf.Context

var mainCmd = &cobra.Command{
	Use:   "surreal",
	Short: "SurrealDB command-line interface and server",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return db.Setup(opts)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return server.Setup(opts)
	},
}

func init() {

	mainCmd.AddCommand(
		sqlCmd,
		importCmd,
		exportCmd,
		versionCmd,
	)

	opts = &cnf.Context{}

	mainCmd.PersistentFlags().StringVarP(&opts.Auth, "auth", "a", "", "Set master authentication details using user:pass format")
	mainCmd.PersistentFlags().StringVarP(&opts.Db, "db", "d", "rpc://node@127.0.0.1:26257", "Set backend datastore")
	mainCmd.PersistentFlags().StringVarP(&opts.Port, "port", "", ":8000", "The host:port on which to serve the web interface")
	mainCmd.PersistentFlags().StringVarP(&opts.Http, "port-http", "", ":33693", "The host:port on which to serve the http sql server")
	mainCmd.PersistentFlags().StringVarP(&opts.Sock, "port-sock", "", ":33793", "The host:port on which to serve the sock sql server")
	mainCmd.PersistentFlags().StringVarP(&opts.Base, "base", "b", "surreal", "Name of the root database key")
	mainCmd.PersistentFlags().BoolVarP(&opts.Verbose, "verbose", "v", false, "Enable verbose output")

	cobra.OnInitialize(setup)

}

// Run runs the cli app
func Run() {
	if err := mainCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(-1)
	}
}
