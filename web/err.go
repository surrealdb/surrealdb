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

package web

import (
	"github.com/abcum/fibre"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
)

func errors(val error, c *fibre.Context) {

	var code int
	var info string

	switch e := val.(type) {
	default:
		code, info = 400, e.Error()
	case *kvs.DBError:
		code, info = 503, e.Error()
	case *kvs.TXError:
		code, info = 500, e.Error()
	case *kvs.KVError:
		code, info = 409, e.Error()
	case *kvs.CKError:
		code, info = 403, e.Error()
	case *sql.NSError:
		code, info = 403, e.Error()
	case *sql.DBError:
		code, info = 403, e.Error()
	case *sql.QueryError:
		code, info = 401, e.Error()
	case *sql.ParseError:
		code, info = 400, e.Error()
	case *fibre.HTTPError:
		code = e.Code()
	}

	if _, ok := errs[code]; !ok {
		code = 500
	}

	c.Send(code, &err{
		errs[code].Code,
		errs[code].Details,
		errs[code].Description,
		info,
	})

}

type err struct {
	Code        int    `codec:"code,omitempty"`
	Details     string `codec:"details,omitempty"`
	Description string `codec:"description,omitempty"`
	Information string `codec:"information,omitempty"`
}

var errs = map[int]*err{

	200: {
		Code:        200,
		Details:     "Information",
		Description: "Visit the documentation for details on accessing the api.",
	},

	400: {
		Code:        400,
		Details:     "Request problems detected",
		Description: "There is a problem with your request. Ensure that the request is valid.",
	},

	401: {
		Code:        401,
		Details:     "Authentication failed",
		Description: "Your authentication details are invalid. Reauthenticate using a valid token.",
	},

	403: {
		Code:        403,
		Details:     "Request resource forbidden",
		Description: "Your request was forbidden. Perhaps you don't have the necessary permissions to access this resource.",
	},

	404: {
		Code:        404,
		Details:     "Request resource not found",
		Description: "The requested resource does not exist. Check that you have entered the url correctly.",
	},

	405: {
		Code:        405,
		Details:     "This method is not allowed",
		Description: "The requested http method is not allowed for this resource. Refer to the documentation for allowed methods.",
	},

	409: {
		Code:        409,
		Details:     "Request conflict detected",
		Description: "The request could not be processed because of a conflict in the request.",
	},

	413: {
		Code:        413,
		Details:     "Request content length too large",
		Description: "All requests to the database must not exceed the predefined content length.",
	},

	415: {
		Code:        415,
		Details:     "Unsupported content type requested",
		Description: "The request needs to adhere to certain constraints. Check your request settings and try again.",
	},

	422: {
		Code:        422,
		Details:     "Request problems detected",
		Description: "There is a problem with your request. The request appears to contain invalid data.",
	},

	426: {
		Code:        426,
		Details:     "Upgrade required",
		Description: "There is a problem with your request. The request is expected to upgrade to a websocket connection.",
	},

	500: {
		Code:        500,
		Details:     "Internal server error",
		Description: "There was a problem with our servers, and we have been notified.",
	},
	},
}
