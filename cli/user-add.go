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

type userAddOptions struct {
	NS string
	DB string
}

var userAddOpt *userAddOptions

var userAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new database user.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func init() {

	userAddOpt = &userAddOptions{}

	userAddCmd.PersistentFlags().StringVar(&userAddOpt.NS, "ns", "", "The path destination for the CA certificate file.")
	userAddCmd.PersistentFlags().StringVar(&userAddOpt.DB, "db", "", "The path destination for the CA private key file.")

}
