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

	"context"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/geof"
)

func geoPoint(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 1:
		if p, ok := ensurePoint(args[0]); ok {
			return p, nil
		}
		if p := ensureFloats(args[0]); len(p) == 2 {
			return sql.NewPoint(p[0], p[1]), nil
		}
	case 2:
		if lat, ok := ensureFloat(args[0]); ok {
			if lng, ok := ensureFloat(args[1]); ok {
				return sql.NewPoint(lat, lng), nil
			}
		}
	}
	return nil, nil
}

func geoCircle(ctx context.Context, args ...interface{}) (interface{}, error) {
	if cen, ok := ensurePoint(args[0]); ok {
		if rad, ok := ensureFloat(args[1]); ok {
			return sql.NewCircle(cen, rad), nil
		}
	}
	if val := ensureFloats(args[0]); len(val) == 2 {
		cen := sql.NewPoint(val[0], val[1])
		if rad, ok := ensureFloat(args[1]); ok {
			return sql.NewCircle(cen, rad), nil
		}
	}
	return nil, nil
}

func geoPolygon(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch len(args) {
	case 0, 2:
		// Not enough arguments, so just ignore
	case 1:
		var pnts []*sql.Point
		if a, ok := ensureSlice(args[0]); ok {
			for _, a := range a {
				if p, _ := ensurePoint(a); p != nil {
					pnts = append(pnts, p)
				} else if p := ensureFloats(a); len(p) == 2 {
					pnts = append(pnts, sql.NewPoint(p[0], p[1]))
				} else {
					return nil, nil
				}
			}
			return sql.NewPolygon(pnts...), nil
		}
	default:
		var pnts []*sql.Point
		for _, a := range args {
			if p, _ := ensurePoint(a); p != nil {
				pnts = append(pnts, p)
			} else if p := ensureFloats(a); len(p) == 2 {
				pnts = append(pnts, sql.NewPoint(p[0], p[1]))
			} else {
				return nil, nil
			}
		}
		return sql.NewPolygon(pnts...), nil
	}
	return nil, nil
}

func geoContains(ctx context.Context, args ...interface{}) (interface{}, error) {
	if a, ok := ensurePolygon(args[0]); ok {
		if b, ok := ensurePoint(args[1]); ok {
			return geof.Contains(a, b), nil
		}
	}
	return false, nil
}

func geoDistance(ctx context.Context, args ...interface{}) (interface{}, error) {
	if pnt, ok := ensurePoint(args[0]); ok {
		if frm, ok := ensurePoint(args[1]); ok {
			return geof.Haversine(pnt, frm), nil
		}
	}
	return -1, nil
}

func geoInside(ctx context.Context, args ...interface{}) (interface{}, error) {
	if a, ok := ensurePoint(args[0]); ok {
		if b, ok := ensurePolygon(args[1]); ok {
			return geof.Inside(a, b), nil
		}
	}
	return false, nil
}

func geoIntersects(ctx context.Context, args ...interface{}) (interface{}, error) {
	if a, ok := ensureGeometry(args[0]); ok {
		if b, ok := ensureGeometry(args[1]); ok {
			return geof.Intersects(a, b), nil
		}
	}
	return false, nil
}

func geoHashDecode(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	return geof.GeohashDecode(s), nil
}

func geoHashEncode(ctx context.Context, args ...interface{}) (interface{}, error) {
	if pnt, ok := ensurePoint(args[0]); ok {
		if acc, ok := ensureInt(args[1]); ok {
			return geof.GeohashEncode(pnt, acc), nil
		}
	}
	return nil, nil
}
