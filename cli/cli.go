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

	"github.com/abcum/surreal/server"
	"github.com/abcum/surreal/stores"

	// Load all backend stores
	_ "github.com/abcum/surreal/stores/boltdb"
	_ "github.com/abcum/surreal/stores/cockroachdb"
	_ "github.com/abcum/surreal/stores/leveldb"
	_ "github.com/abcum/surreal/stores/memory"
	_ "github.com/abcum/surreal/stores/mongodb"
	_ "github.com/abcum/surreal/stores/rethinkdb"
)

var mainCmd = &cobra.Command{
	Use: "surreal",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return stores.Setup(Config.Context)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return server.Setup(Config.Context)
	},
}

func init() {

	mainCmd.AddCommand(
		kvCmd,
		sqlCmd,
		importCmd,
		exportCmd,
		versionCmd,
	)

	mainCmd.PersistentFlags().StringVarP(&Config.Db, "db", "d", "memory", "Set backend datastore")
	mainCmd.PersistentFlags().StringVarP(&Config.DbPath, "dbpath", "", "", "Set path to boltdb/leveldb datastore file")
	mainCmd.PersistentFlags().StringVarP(&Config.DbName, "dbname", "", "", "Set name of mongodb/rethinkdb database table")
	mainCmd.PersistentFlags().StringVarP(&Config.Http, "http", "", ":33693", "Host to listen for http connections on")
	mainCmd.PersistentFlags().StringVarP(&Config.Sock, "sock", "", ":33793", "Port to listen for sock connections on")
	mainCmd.PersistentFlags().StringVarP(&Config.Base, "base", "b", "surreal", "Name of the root database key")
	mainCmd.PersistentFlags().BoolVarP(&Config.Verbose, "verbose", "v", false, "Enable verbose output")

}

// Run runs the cli app
func Run() {
	if err := mainCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(-1)
	}
}
