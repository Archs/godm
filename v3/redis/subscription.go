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

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// SUBSCRIPTION
//--------------------

// Subscription manages a subscription to Redis channels and allows
// to subscribe and unsubscribe from channels.
type Subscription struct {
	database *Database
	resp     *resp
}

// newSubscription creates a new subscription.
func newSubscription(db *Database, r *resp) (*Subscription, error) {
	sub := &Subscription{
		database: db,
		resp:     r,
	}
	// Perform authentication and database selection.
	err := sub.resp.authenticate()
	if err != nil {
		sub.database.pool.kill(r)
		return nil, err
	}
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
func (s *Subscription) Pop() (*PublishedValue, error) {
	result, err := s.resp.receiveResultSet()
	if err != nil {
		return nil, err
	}
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

// Close ends the subscription.
func (s *Subscription) Close() error {
	err := s.resp.sendCommand("punsubscribe")
	if err != nil {
		return err
	}
	for {
		pv, err := s.Pop()
		if err != nil {
			return err
		}
		if pv.Kind == "punsubscribe" {
			break
		}
	}
	s.database.pool.push(s.resp)
	return nil
}

// EOF
