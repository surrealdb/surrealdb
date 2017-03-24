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

package tcp

import (
	"time"

	"io/ioutil"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/log"

	"github.com/hashicorp/serf/serf"
)

var srf *serf.Serf

// Setup sets up the server for remote connections
func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("tcp").Infof("Starting tcp server on %s", opts.Conn.Tcp)

	chn := make(chan serf.Event)

	cfg := serf.DefaultConfig()
	cfg.EventCh = chn
	cfg.NodeName = opts.Node.UUID
	cfg.LogOutput = ioutil.Discard
	cfg.ReconnectTimeout = 60 * time.Second
	cfg.TombstoneTimeout = 60 * time.Second

	cfg.MemberlistConfig.LogOutput = ioutil.Discard

	cfg.MemberlistConfig.SecretKey = opts.DB.Key
	cfg.MemberlistConfig.BindPort = opts.Port.Tcp
	cfg.MemberlistConfig.AdvertisePort = opts.Port.Tcp

	srf, err = serf.Create(cfg)
	if len(opts.Node.Join) > 0 {
		if _, err := srf.Join(opts.Node.Join, true); err != nil {
			log.Infoln(err)
		}
	}

	go func() {
		for evt := range chn {
			switch evt.EventType() {
			case serf.EventMemberReap:
				msg := evt.(serf.MemberEvent)
				for _, member := range msg.Members {
					log.WithPrefix("tcp").Debugf("Cluster member reaped: %s:%d", member.Addr, member.Port)
				}
			case serf.EventMemberJoin:
				msg := evt.(serf.MemberEvent)
				for _, member := range msg.Members {
					log.WithPrefix("tcp").Debugf("Cluster member joined: %s:%d", member.Addr, member.Port)
				}
			case serf.EventMemberLeave:
				msg := evt.(serf.MemberEvent)
				for _, member := range msg.Members {
					log.WithPrefix("tcp").Debugf("Cluster member exited: %s:%d", member.Addr, member.Port)
				}
			case serf.EventMemberFailed:
				msg := evt.(serf.MemberEvent)
				for _, member := range msg.Members {
					log.WithPrefix("tcp").Debugf("Cluster member failed: %s:%d", member.Addr, member.Port)
				}
			case serf.EventUser:
				msg := evt.(serf.UserEvent)
				log.WithPrefix("tcp").Debugf("Received user event: %v with payload %s", msg.Name, msg.Payload)
			case serf.EventQuery:
				msg := evt.(*serf.Query)
				log.WithPrefix("tcp").Debugf("Received query event: %v with payload %s", msg.Name, msg.Payload)
			}
		}
	}()

	// Log successful start

	log.WithPrefix("tcp").Infof("Started tcp server on %s", opts.Conn.Tcp)

	return

}

func Send(name string, data []byte) {
	srf.UserEvent(name, data, false)
}

// Exit tears down the server gracefully
func Exit() {
	log.WithPrefix("tcp").Infof("Gracefully shutting down %s protocol", "tcp")
	srf.Leave()
}
