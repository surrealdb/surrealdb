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
	"net/http"
	"os"

	"io/ioutil"

	"github.com/spf13/cobra"

	"github.com/abcum/surreal/log"
)

var (
	importUser string
	importPass string
	importConn string
	importNS   string
	importDB   string
)

var importCmd = &cobra.Command{
	Use:     "import [flags] <file>",
	Short:   "Execute a SQL script against an existing database",
	Example: "  surreal import --auth root:root backup.sql",
	RunE: func(cmd *cobra.Command, args []string) (err error) {

		var fle *os.File
		var req *http.Request
		var res *http.Response

		// Ensure that the command has a filepath
		// as the output file argument. If no filepath
		// has been provided then return an error.

		if len(args) != 1 {
			log.Fatalln("No filepath provided.")
			return
		}

		// Attempt to open or create the specified file
		// in write-only mode, and if there is a problem
		// creating the file, then return an error.

		if fle, err = os.OpenFile(args[0], os.O_RDONLY, 0644); err != nil {
			log.Fatalln("SQL failed - please check the filepath and try again.")
			return
		}

		defer fle.Close()

		// Configure the sql connection endpoint url
		// and specify the authentication header using
		// basic auth for root login.

		url := fmt.Sprintf("%s/sql", importConn)

		if req, err = http.NewRequest("POST", url, fle); err != nil {
			log.Fatalln("Connection failed - check the connection details and try again.")
			return
		}

		// Specify that the request is plain text

		req.Header.Set("Content-Type", "text/plain")

		// Specify the db authentication settings

		req.SetBasicAuth(importUser, importPass)

		// Specify the namespace to import

		req.Header.Set("NS", importNS)

		// Specify the database to import

		req.Header.Set("DB", importDB)

		// Attempt to dial the sql endpoint and
		// if there is an error then stop execution
		// and return the connection error.

		if res, err = http.DefaultClient.Do(req); err != nil {
			log.Fatalln("Connection failed - check the connection details and try again.")
			return
		}

		// Ensure that we close the body, otherwise
		// if the Body is not closed, the Client can
		// not use the underlying transport again.

		defer res.Body.Close()

		// Ensure that we didn't receive a 401 status
		// code back from the server, otherwise there
		// was a problem with our authentication.

		if res.StatusCode == 401 {
			log.Fatalln("Authentication failed - check the connection details and try again.")
			return
		}

		// Ensure that we received a http 200 status
		// code back from the server, otherwise there
		// was a problem with our request.

		if res.StatusCode != 200 {
			bdy, _ := ioutil.ReadAll(res.Body)
			log.Fatalf("%s", bdy)
			return
		}

		return

	},
}

func init() {

	importCmd.PersistentFlags().StringVarP(&importUser, "user", "u", "root", "Database authentication username to use when connecting.")
	importCmd.PersistentFlags().StringVarP(&importPass, "pass", "p", "pass", "Database authentication password to use when connecting.")
	importCmd.PersistentFlags().StringVarP(&importConn, "conn", "c", "https://surreal.io", "Remote database server url to connect to.")
	importCmd.PersistentFlags().StringVar(&importNS, "ns", "", "Master authentication details to use when connecting.")
	importCmd.PersistentFlags().StringVar(&importDB, "db", "", "Master authentication details to use when connecting.")

}
