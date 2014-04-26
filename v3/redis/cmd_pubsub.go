// Tideland Go Data Management - Redis Client - Commands - Pub/Sub
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import (
	"strings"
	"sync"

	"github.com/tideland/goas/v2/identifier"
	"github.com/tideland/goas/v2/monitoring"
)

//--------------------
// CONNECTION
//--------------------

// subscription subscribes or unsubscribes this connector to a number of
// channels and returns the number of currently subscribed channels.
func (conn *Connection) subscription(publishings chan *PublishedValue, cmd string, channels ...string) (int, error) {
	cmd = strings.ToLower(cmd)
	for _, channel := range channels {
		if containsPattern(channel) {
			cmd = "p" + cmd
			break
		}
	}
	args := buildInterfaces(channels)
	if conn.database.monitoring {
		m := monitoring.BeginMeasuring(identifier.Identifier("redis", "command", cmd))
		defer m.EndMeasuring()
	}
	results, err := conn.request(cmd, args, publishings)
	if err != nil {
		return -1, err
	}
	return results.IntAt(0)
}

// Publish posts a message to the given channel and returns the numer of receivers.
func (conn *Connection) Publish(channel string, message interface{}) (int, error) {
	results, err := conn.Command("publish", channel, message)
	if err != nil {
		return 0, err
	}
	return results.IntAt(0)
}

//--------------------
// SUBSCRIPTION
//--------------------

// Subscription manages a subscription to Redis channels and allows
// to subscribe and unsubscribe from channels.
type Subscription struct {
	mux         sync.Mutex
	pool        *pool
	conn        *Connection
	channels    map[string]bool
	count       int
	publishings chan *PublishedValue
}

// newSubscription creates a new subscription.
func newSubscription(pool *pool) (*Subscription, error) {
	conn, err := pool.pull(forcedPull)
	if err != nil {
		return nil, err
	}
	sub := &Subscription{
		pool:        pool,
		conn:        conn,
		channels:    make(map[string]bool),
		count:       0,
		publishings: make(chan *PublishedValue),
	}
	return sub, nil
}

// Subscribe adds one or more channels to the subscription.
func (s *Subscription) Subscribe(channels ...string) (int, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	count, err := s.conn.subscription(s.publishings, "subscribe", channels...)
	if err != nil {
		return count, err
	}
	for _, channel := range channels {
		s.channels[channel] = true
	}
	s.count = count
	return s.count, nil
}

// Unsubscribe removes one or more channels from the subscription.
func (s *Subscription) Unsubscribe(channels ...string) (int, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	count, err := s.conn.subscription(s.publishings, "unsubscribe", channels...)
	if err != nil {
		return count, err
	}
	for _, channel := range channels {
		delete(s.channels, channel)
	}
	s.count = count
	return s.count, nil
}

// ChannelCount returns the number of subscribed channels.
func (s *Subscription) ChannelCount() int {
	s.mux.Lock()
	defer s.mux.Unlock()

	return s.count
}

// Publishings returns a channel emitting the published values.
func (s *Subscription) Publishings() <-chan *PublishedValue {
	return s.publishings
}

// Close ends the subscription.
func (s *Subscription) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.conn.Command("punsubscribe")
	close(s.publishings)
	return s.pool.push(s.conn)
}

// EOF
