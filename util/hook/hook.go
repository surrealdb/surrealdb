// Copyright © 2016 SurrealDB Ltd.
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

package hook

import (
	"context"
	"time"
)

const (
	Static Kind = iota
	Backoff
)

// Kind specifies the policy type.
type Kind int

// Policy represents a retryable function policy.
type Policy struct {
	// Type is the policy type
	Type Kind
	// Attempts to retry
	Retry int
	// Factor is the backoff rate
	Factor int
	// Sleep is the initial duration to wait before retrying
	Sleep time.Duration
}

// New creates a new static retryable policy, which retries
// after the duration of 'sleep', until the number of retries
// has been reached.
func NewStatic(retry int, sleep time.Duration) *Policy {
	return &Policy{
		Type:  Static,
		Retry: retry,
		Sleep: sleep,
	}
}

// New creates a new backoff retryable policy, which increases
// the delay between subsequent retries by the secified factor,
// until the number of retries has been reached.
func NewBackoff(retry, factor int, sleep time.Duration) *Policy {
	return &Policy{
		Type:   Backoff,
		Retry:  retry,
		Sleep:  sleep,
		Factor: factor,
	}
}

// Run executes a function until:
// 1. A nil error is returned,
// 2. The max number of retries has been reached,
// 3. The specified context has been cancelled or timedout.
func (p *Policy) Run(ctx context.Context, fnc func() error) error {

	c := make(chan error, 1)

	go func() { c <- p.run(ctx, fnc) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c:
		return err
	}

}

func (p *Policy) run(ctx context.Context, fnc func() error) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if err := fnc(); err != nil {
			if p.Retry > 0 {
				p.sleep()
				p.Retry = p.Retry - 1
				return p.run(ctx, fnc)
			}
		}
	}

	return nil

}

func (p *Policy) sleep() {

	time.Sleep(p.Sleep)

	if p.Type == Backoff {
		p.Sleep = p.Sleep * time.Duration(p.Factor)
	}

}
