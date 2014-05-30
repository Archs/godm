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

import (
	"strings"

	"github.com/tideland/goas/v2/loop"
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// SUBSCRIPTION
//--------------------

// Subscription manages a subscription to Redis channels and allows
// to subscribe and unsubscribe from channels.
type Subscription struct {
	database    *Database
	resp        *resp
	publishings *publishedValues
	loop        loop.Loop
}

// newSubscription creates a new subscription.
func newSubscription(db *Database, r *resp) (*Subscription, error) {
	sub := &Subscription{
		database:    db,
		resp:        r,
		publishings: newPublishedValues(),
	}
	sub.loop = loop.Go(sub.backendLoop)
	return sub, nil
}

// Subscribe adds one or more channels to the subscription.
func (s *Subscription) Subscribe(channels ...string) error {
	return s.subUnsub("subscribe", channels...)
}

// Unsubscribe removes one or more channels from the subscription.
func (s *Subscription) Unsubscribe(channels ...string) error {
	return s.subUnsub("unsubscribe", channels...)
}

// subUnsub is the generic subscription and unsubscription method.
func (s *Subscription) subUnsub(cmd string, channels ...string) error {
	pattern := false
	args := []interface{}{}
	for _, channel := range channels {
		if containsPattern(channel) {
			pattern = true
		}
		args = append(args, channel)
	}
	if pattern {
		cmd = "p" + cmd
	}
	return s.resp.sendCommand(cmd, args...)
}

// Pop waits for a published value and returns it.
func (s *Subscription) Pop() *PublishedValue {
	return s.publishings.Dequeue()
}

// Close ends the subscription.
func (s *Subscription) Close() error {
	s.resp.sendCommand("punsubscribe")
	s.loop.Stop()
	s.publishings.Close()
	return s.database.pool.push(s.resp)
}

// backendLoop receives the responses of the server and
// adds them to the published values.
func (s *Subscription) backendLoop(loop loop.Loop) error {
	for {
		select {
		case <-s.loop.ShallStop():
			return nil
		default:
			pv, err := s.receivePublishedValue()
			if err != nil {
				return err
			}
			err = s.publishings.Enqueue(pv)
			if err != nil {
				return err
			}
		}
	}
}

// receivePublishedValue
func (s *Subscription) receivePublishedValue() (*PublishedValue, error) {
	result, err := s.resp.receiveResultSet()
	// Analyse the result.
	kind, err := result.StringAt(0)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.Contains(kind, "message"):
		channel, err := result.StringAt(1)
		if err != nil {
			return nil, err
		}
		value, err := result.ValueAt(2)
		if err != nil {
			return nil, err
		}
		return &PublishedValue{
			Kind:    kind,
			Channel: channel,
			Value:   value,
		}, nil
	case strings.Contains(kind, "subscribe"):
		channel, err := result.StringAt(1)
		if err != nil {
			return nil, err
		}
		count, err := result.IntAt(2)
		if err != nil {
			return nil, err
		}
		return &PublishedValue{
			Kind:    kind,
			Channel: channel,
			Count:   count,
		}, nil
	default:
		return nil, errors.New(ErrInvalidResponse, errorMessages, result)
	}
}

// EOF
