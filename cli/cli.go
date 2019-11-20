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
	"os"

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/cnf"
)

var opts *cnf.Options

var mainCmd = &cobra.Command{
	Use:   "surreal",
	Short: "SurrealDB command-line interface and server",
}

func init() {

	mainCmd.AddCommand(
		startCmd,
		importCmd,
		exportCmd,
		versionCmd,
	)

	opts = &cnf.Options{}

	mainCmd.PersistentFlags().StringVar(&opts.Logging.Level, "log-level", "error", "Specify log verbosity")
	mainCmd.PersistentFlags().StringVar(&opts.Logging.Output, "log-output", "stderr", "Specify log output destination")
	mainCmd.PersistentFlags().StringVar(&opts.Logging.Format, "log-format", "text", "Specify log output format (text, json)")

	cobra.OnInitialize(setup)

}

// Init runs the cli app
func Init() {
	if err := mainCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
