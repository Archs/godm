// Tideland Go Data Management - Redis Client - Subscription
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import ()

//--------------------
// SUBSCRIPTION
//--------------------

// Subscription manages a subscription to Redis channels and allows
// to subscribe and unsubscribe from channels.
type Subscription struct {
	database    *Database
	resp        *RESP
	publishings *publishedValues
}

// newSubscription creates a new subscription.
func newSubscription(db *Database, resp *RESP) (*Subscription, error) {
	sub := &Subscription{
		database:    db,
		resp:        resp,
		publishings: newPublishedValues(),
	}
	return sub, nil
}

// Subscribe adds one or more channels to the subscription.
func (s *Subscription) Subscribe(channels ...string) error {
	return nil
}

// Unsubscribe removes one or more channels from the subscription.
func (s *Subscription) Unsubscribe(channels ...string) error {
	return nil
}

// Pop waits for a published value and returns it.
func (s *Subscription) Pop() *PublishedValue {
	return s.publishings.Dequeue()
}

// Close ends the subscription.
func (s *Subscription) Close() error {
	// s.conn.Do("punsubscribe")
	s.publishings.Close()
	return s.database.pool.push(s.resp)
}

// EOF
