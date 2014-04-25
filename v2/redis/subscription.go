// Tideland Go Data Management - Redis Client - Subscription
//
// Copyright (C) 2009-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import (
	"sync"
)

//--------------------
// SUBSCRIPTION
//--------------------

// Subscription manages a subscription to Redis channels and allows
// to subscribe and unsubscribe from channels.
type Subscription interface {
	// Subscribe adds one or more channels to the subscription.
	Subscribe(channels ...string) (int, error)

	// Unsubscribe removes one or more channels from the subscription.
	Unsubscribe(channels ...string) (int, error)

	// ChannelCount returns the number of subscribed channels.
	ChannelCount() int

	// Publishings returns a channel emitting the published values.
	Publishings() <-chan PublishedValue

	// Close ends the subscription.
	Close() error
}

// Subscription implements the Subscription interface.
type subscription struct {
	mux         sync.Mutex
	cnctr       *connector
	count       int
	publishings chan PublishedValue
}

// newSubscription creates a new subscription.
func newSubscription(cnctr *connector, channels ...string) (Subscription, error) {
	publishings := make(chan PublishedValue)
	count, err := cnctr.subscription(publishings, "subscribe", channels...)
	if err != nil {
		return nil, err
	}
	sub := &subscription{
		cnctr:       cnctr,
		count:       count,
		publishings: publishings,
	}
	return sub, nil
}

func (s *subscription) Subscribe(channels ...string) (int, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	count, err := s.cnctr.subscription(s.publishings, "subscribe", channels...)
	if err != nil {
		return count, err
	}
	s.count = count
	return s.count, nil
}

func (s *subscription) Unsubscribe(channels ...string) (int, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	count, err := s.cnctr.subscription(s.publishings, "unsubscribe", channels...)
	if err != nil {
		return count, err
	}
	s.count = count
	return s.count, nil
}

func (s *subscription) ChannelCount() int {
	s.mux.Lock()
	defer s.mux.Unlock()

	return s.count
}

func (s *subscription) Publishings() <-chan PublishedValue {
	return s.publishings
}

func (s *subscription) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	close(s.publishings)
	return s.cnctr.stop()
}

// EOF
