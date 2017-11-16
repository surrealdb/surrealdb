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

package fncs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"encoding/json"

	"golang.org/x/net/context/ctxhttp"

	"github.com/abcum/surreal/util/build"
	"github.com/abcum/surreal/util/hook"
)

type opts map[string]interface{}

var version = build.GetInfo().Ver

var httpResponseError = errors.New("HTTP response error")

func httpHead(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runSync(ctx, "HEAD", args...)
}

func httpGet(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runSync(ctx, "GET", args...)
}

func httpPut(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runSync(ctx, "PUT", args...)
}

func httpPost(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runSync(ctx, "POST", args...)
}

func httpPatch(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runSync(ctx, "PATCH", args...)
}

func httpDelete(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runSync(ctx, "DELETE", args...)
}

func httpAsyncHead(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runAsync(ctx, "HEAD", args...)
}

func httpAsyncGet(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runAsync(ctx, "GET", args...)
}

func httpAsyncPut(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runAsync(ctx, "PUT", args...)
}

func httpAsyncPost(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runAsync(ctx, "POST", args...)
}

func httpAsyncPatch(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runAsync(ctx, "PATCH", args...)
}

func httpAsyncDelete(ctx context.Context, args ...interface{}) (interface{}, error) {
	return runAsync(ctx, "DELETE", args...)
}

func runSync(ctx context.Context, met string, args ...interface{}) (interface{}, error) {
	url, bdy, opt := httpCnf(met, args...)
	out, _ := httpRes(ctx, met, url, bdy, opt)
	return out, nil
}

func runAsync(ctx context.Context, met string, args ...interface{}) (interface{}, error) {
	url, bdy, opt := httpCnf(met, args...)
	ctx = context.Background()
	go hook.NewBackoff(5, 5, 10*time.Second).Run(ctx, func() error {
		return httpErr(httpReq(ctx, met, url, bdy, opt))
	})
	return nil, nil
}

func httpCnf(met string, args ...interface{}) (url string, bdy io.Reader, opt opts) {
	var bit []byte
	switch met {
	case "HEAD", "GET", "DELETE":
		switch len(args) {
		case 1:
			url, _ = ensureString(args[0])
		case 2:
			url, _ = ensureString(args[0])
			opt, _ = ensureObject(args[1])
		}
	case "PUT", "POST", "PATCH":
		switch len(args) {
		case 1:
			url, _ = ensureString(args[0])
		case 2:
			url, _ = ensureString(args[0])
			switch v := args[1].(type) {
			case []interface{}, map[string]interface{}:
				bit, _ = json.Marshal(v)
			default:
				bit, _ = ensureBytes(v)
			}
		case 3:
			url, _ = ensureString(args[0])
			switch v := args[1].(type) {
			case []interface{}, map[string]interface{}:
				bit, _ = json.Marshal(v)
			default:
				bit, _ = ensureBytes(v)
			}
			opt, _ = ensureObject(args[2])
		}
	}
	return url, bytes.NewReader(bit), opt
}

func httpReq(ctx context.Context, met, url string, body io.Reader, conf opts) (*http.Response, error) {

	cli := new(http.Client)

	req, err := http.NewRequest(met, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "SurrealDB HTTP/"+version)

	if val, ok := conf["auth"]; ok {
		if opt, ok := ensureObject(val); ok {
			user, _ := ensureString(opt["user"])
			pass, _ := ensureString(opt["pass"])
			req.SetBasicAuth(user, pass)
		}
	}

	if val, ok := conf["head"]; ok {
		if opt, ok := ensureObject(val); ok {
			for key, v := range opt {
				head, _ := ensureString(v)
				req.Header.Set(key, head)
			}
		}
	}

	res, err := ctxhttp.Do(ctx, cli, req)
	if err != nil {
		return nil, err
	}

	return res, nil

}

func httpRes(ctx context.Context, met, url string, body io.Reader, conf opts) (interface{}, error) {

	var out interface{}

	res, err := httpReq(ctx, met, url, body, conf)
	if err != nil {
		return nil, err
	}

	bdy, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bdy, &out)
	if err != nil {
		if len(bdy) != 0 {
			return bdy, nil
		}
	}

	return out, nil

}

func httpErr(res *http.Response, err error) error {
	if err != nil {
		return err
	}
	if res.StatusCode >= 500 {
		return httpResponseError
	}
	return nil
}
