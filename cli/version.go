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
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/util/build"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Output version information",
	Run: func(cmd *cobra.Command, args []string) {

		info := build.GetInfo()

		tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(tw, "Build Go:    %s\n", info.Go)
		fmt.Fprintf(tw, "Build Ver:   %s\n", info.Ver)
		fmt.Fprintf(tw, "Build Rev:   %s\n", info.Rev)
		fmt.Fprintf(tw, "Build Time:  %s\n", info.Time)

		tw.Flush()

	},
}
