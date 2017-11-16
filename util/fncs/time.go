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
	"time"
)

func timeNow(ctx context.Context, args ...interface{}) (time.Time, error) {
	return time.Now(), nil
}

func timeAdd(ctx context.Context, args ...interface{}) (time.Time, error) {
	if t, ok := ensureTime(args[0]); ok {
		if d, ok := ensureDuration(args[1]); ok {
			return t.Add(d), nil
		}
	}
	return time.Unix(0, 0), nil
}

func timeAge(ctx context.Context, args ...interface{}) (time.Time, error) {
	if t, ok := ensureTime(args[0]); ok {
		if d, ok := ensureDuration(args[1]); ok {
			return t.Add(-d), nil
		}
	}
	return time.Unix(0, 0), nil
}

func timeFloor(ctx context.Context, args ...interface{}) (interface{}, error) {
	if t, ok := ensureTime(args[0]); ok {
		if d, ok := ensureDuration(args[1]); ok {
			return t.Truncate(d), nil
		}
	}
	return nil, nil
}

func timeRound(ctx context.Context, args ...interface{}) (interface{}, error) {
	if t, ok := ensureTime(args[0]); ok {
		if d, ok := ensureDuration(args[1]); ok {
			return t.Round(d), nil
		}
	}
	return nil, nil
}

func timeDay(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().Day()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.Day()), nil
		}
	}
	return 0, nil
}

func timeHour(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().Hour()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.Hour()), nil
		}
	}
	return 0, nil
}

func timeMins(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().Minute()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.Minute()), nil
		}
	}
	return 0, nil
}

func timeMonth(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().Month()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.Month()), nil
		}
	}
	return 0, nil
}

func timeNano(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().UnixNano()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.UnixNano()), nil
		}
	}
	return 0, nil
}

func timeSecs(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().Second()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.Second()), nil
		}
	}
	return 0, nil
}

func timeUnix(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().Unix()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.Unix()), nil
		}
	}
	return 0, nil
}

func timeYear(ctx context.Context, args ...interface{}) (float64, error) {
	switch len(args) {
	case 0:
		return float64(time.Now().Year()), nil
	case 1:
		if v, ok := ensureTime(args[0]); ok {
			return float64(v.Year()), nil
		}
	}
	return 0, nil
}
