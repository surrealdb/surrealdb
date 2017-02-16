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
)

var importCmd = &cobra.Command{
	Use:     "import",
	Short:   "Import data into an existing database",
	Example: "  surreal import",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

func init() {

	importCmd.PersistentFlags().StringVar(&opts.Auth.Auth, "auth", "root:root", "Master authentication details to use when connecting.")
	importCmd.PersistentFlags().StringVar(&opts.DB.Host, "host", "127.0.0.1", "Database server host to connect to.")
	importCmd.PersistentFlags().StringVar(&opts.DB.Port, "port", "8000", "Database server port to connect to.")

}
