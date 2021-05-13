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
	"math"

	"github.com/abcum/surreal/sql"
)

func raycast(point, beg, end *sql.Point) bool {

	// Always ensure that the the first point
	// has a Y coordinate that is less than
	// the second point. Switch if not.

	if beg.LO > end.LO {
		beg, end = end, beg
	}

	// Move the point's Y coordinate outside of
	// the bounds of the testing region so that
	// we can start drawing a ray
	for point.LO == beg.LO || point.LO == end.LO {
		lng := math.Nextafter(point.LO, math.Inf(1))
		point = sql.NewPoint(point.LA, lng)
	}

	// If we are outside of the polygon, indicate so.
	if point.LO < beg.LO || point.LO > end.LO {
		return false
	}

	if beg.LA > end.LA {
		if point.LA > beg.LA {
			return false
		}
		if point.LA < end.LA {
			return true
		}

	} else {
		if point.LA > end.LA {
			return false
		}
		if point.LA < beg.LA {
			return true
		}
	}

	raySlope := (point.LO - beg.LO) / (point.LA - beg.LA)
	diagSlope := (end.LO - beg.LO) / (end.LA - beg.LA)

	return raySlope >= diagSlope

}
