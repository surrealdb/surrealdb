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

package server

import (
	"github.com/labstack/echo"
)

func show(i interface{}) interface{} {
	return i
}

func oops(e error) interface{} {
	return map[string]interface{}{
		"code":          400,
		"details":       "Request problems detected",
		"documentation": docs,
		"information":   e.Error(),
	}
}

func errors(err error, c *echo.Context) {

	code := 500

	if e, ok := err.(*echo.HTTPError); ok {
		code = e.Code()
	}

	c.JSON(code, errs[code])

}

const docs = "https://docs.surreal.io/"

var errs = map[int]interface{}{

	200: map[string]interface{}{
		"code":          200,
		"details":       "Information",
		"documentation": docs,
		"information":   "Visit the documentation for details on accessing the api.",
	},

	400: map[string]interface{}{
		"code":          400,
		"details":       "Request problems detected",
		"documentation": docs,
		"information":   "There is a problem with your request. The request needs to adhere to certain constraints.",
	},

	401: map[string]interface{}{
		"code":          401,
		"details":       "Authentication failed",
		"documentation": docs,
		"information":   "Your authentication details are invalid. Reauthenticate using a valid token.",
	},

	403: map[string]interface{}{
		"code":          403,
		"details":       "Request resource forbidden",
		"documentation": docs,
		"information":   "Your request was forbidden. Perhaps you don't have the necessary permissions to access this resource.",
	},

	404: map[string]interface{}{
		"code":          404,
		"details":       "Request resource not found",
		"documentation": docs,
		"information":   "The requested resource does not exist. Check that you have entered the url correctly.",
	},

	405: map[string]interface{}{
		"code":          405,
		"details":       "This method is not allowed",
		"documentation": docs,
		"information":   "The requested http method is not allowed for this resource. Refer to the documentation for allowed methods.",
	},

	409: map[string]interface{}{
		"code":          409,
		"details":       "Request conflict detected",
		"documentation": docs,
		"information":   "The request could not be processed because of a conflict in the request.",
	},

	413: map[string]interface{}{
		"code":          413,
		"details":       "Request content length too large",
		"documentation": docs,
		"information":   "All SQL requests to the database must not exceed the predefined content length.",
	},

	415: map[string]interface{}{
		"code":          415,
		"details":       "Unsupported content type requested",
		"documentation": docs,
		"information":   "Requests to the database must use the 'Content-Type: application/json' header. Check your request settings and try again.",
	},

	500: map[string]interface{}{
		"code":          500,
		"details":       "There was a problem with our servers, and we have been notified",
		"documentation": docs,
	},
}
