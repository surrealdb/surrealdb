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

var kvCmd = &cobra.Command{
	Use:   "kv",
	Short: "Open a kv connection to the database",
}

func init() {

	kvCmd.AddCommand(
		getCmd,
		putCmd,
		delCmd,
		delrCmd,
		scanCmd,
		rscanCmd,
	)

}

var getCmd = &cobra.Command{
	Use:   "get [options] <key>",
	Short: "Gets the value for a key",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

var putCmd = &cobra.Command{
	Use:   "put [options] <key>",
	Short: "Puts the value for a key",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

var delCmd = &cobra.Command{
	Use:   "del [options] <key>",
	Short: "Deletes a key",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

var delrCmd = &cobra.Command{
	Use:   "delr [options] [<start-key> [<end-key>]]",
	Short: "Deletes a range of keys",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

var scanCmd = &cobra.Command{
	Use:   "scan [options] [<start-key> [<end-key>]]",
	Short: "Scans a range of keys",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

var rscanCmd = &cobra.Command{
	Use:   "rscan [options] [<start-key> [<end-key>]]",
	Short: "Scans a range of keys in reverse",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}
