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

package geof

import (
	"bytes"

	"github.com/abcum/surreal/sql"
)

var bits = []int{16, 8, 4, 2, 1}

var latmax = []float64{-90, 90}

var lngmax = []float64{-180, 180}

var base32 = []byte("0123456789bcdefghjkmnpqrstuvwxyz")

func refine(interval []float64, cd, mask int) []float64 {
	if cd&mask > 0 {
		interval[0] = (interval[0] + interval[1]) / 2
	} else {
		interval[1] = (interval[0] + interval[1]) / 2
	}
	return interval
}

func GeohashDecode(hash string) *sql.Point {

	isEven := true
	lat := latmax
	lng := lngmax
	latErr := float64(90)
	lngErr := float64(180)
	var c string
	var cd int

	for i := 0; i < len(hash); i++ {
		c = hash[i : i+1]
		cd = bytes.Index(base32, []byte(c))
		for j := 0; j < 5; j++ {
			if isEven {
				lngErr /= 2
				lng = refine(lng, cd, bits[j])
			} else {
				latErr /= 2
				lat = refine(lat, cd, bits[j])
			}
			isEven = !isEven
		}
	}

	return sql.NewPoint(
		(lat[0]+lat[1])/2,
		(lng[0]+lng[1])/2,
	)

}

func GeohashEncode(point *sql.Point, precision int64) string {

	isEven := true
	lat := []float64{-90, 90}
	lng := []float64{-180, 180}
	bit := 0
	ch := 0
	var geohash bytes.Buffer
	var mid float64
	for geohash.Len() < int(precision) {
		if isEven {
			mid = (lng[0] + lng[1]) / 2
			if point.LO > mid {
				ch |= bits[bit]
				lng[0] = mid
			} else {
				lng[1] = mid
			}
		} else {
			mid = (lat[0] + lat[1]) / 2
			if point.LA > mid {
				ch |= bits[bit]
				lat[0] = mid
			} else {
				lat[1] = mid
			}
		}
		isEven = !isEven
		if bit < 4 {
			bit++
		} else {
			geohash.WriteByte(base32[ch])
			bit = 0
			ch = 0
		}
	}

	return geohash.String()

}
