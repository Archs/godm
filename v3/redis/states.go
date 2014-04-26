// Tideland Go Data Management - Redis Client - Connection States
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
	"fmt"
	"strings"

	"github.com/tideland/goas/v2/logger"
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// ENVELOPES
//--------------------

// responseEnv encapsulates the response to the caller
// of a command or a subscriber.
type responseEnv struct {
	result *ResultSet
	err    error
}

// requestEnv encapsulates a request for the Redis server.
type requestEnv struct {
	command     string
	arguments   []interface{}
	responses   chan *responseEnv
	publishings chan *PublishedValue
}

// String creates a string representation of the request.
func (r *requestEnv) String() string {
	return fmt.Sprintf("REQUEST (CMD: %s ARGS: %v)", r.command, r.arguments)
}

//--------------------
// STATE
//--------------------

// state represents a connector state and is responsible
// for handling a response.
type state interface {
	// handle handles a request or reply and returns the next state.
	handle(conn *Connection, request *requestEnv, reply *replyEnv) (state, error)
}

//--------------------
// IDLING STATE
//--------------------

// idlingState represents an idling connector.
type idlingState struct{}

func (st *idlingState) handle(conn *Connection, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// Send command to the server.
		if err := conn.sendCommand(request.command, request.arguments); err != nil {
			return nil, err
		}
		if strings.Contains(request.command, "subscribe") {
			// Anyone of the (un-)subscribe commands.
			return newSubscriptionState(request, len(request.arguments)), nil
		}
		// A regular command.
		return newCommandState(request), nil
	case reply != nil:
		// No reply expected here.
		return nil, errors.New(ErrUnexpectedReply, errorMessages, reply)
	}
	return st, nil
}

//--------------------
// COMMAND STATE
//--------------------

// commandState represents a connector waiting for a
// single response after sending a command.
type commandState struct {
	request *requestEnv
	result  *ResultSet
	current *ResultSet
}

func newCommandState(request *requestEnv) *commandState {
	st := &commandState{
		request: request,
		result:  newResultSet(),
	}
	st.current = st.result
	return st
}

func (st *commandState) handle(conn *Connection, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// No request expected here.
		return nil, errors.New(ErrUnexpectedRequest, errorMessages, request)
	case reply != nil:
		// Handle reply.
		switch reply.kind {
		case receivingError:
			st.request.responses <- &responseEnv{nil, reply.err}
			return &idlingState{}, nil
		case timeoutError:
			st.request.responses <- &responseEnv{nil, errors.New(ErrTimeout, errorMessages, st.request.command)}
			return &idlingState{}, nil
		case errorReply:
			st.request.responses <- &responseEnv{nil, errors.New(ErrServerResponse, errorMessages, reply.value())}
			return &idlingState{}, nil
		case statusReply, integerReply, bulkReply, nullBulkReply:
			st.current.append(reply.value())
		case arrayReply:
			switch {
			case st.current == st.result && st.current.Len() == 0:
				st.current.length = reply.length
			case !st.current.allReceived():
				next := newResultSet()
				next.parent = st.current
				st.current.append(next)
				st.current = next
				st.current.length = reply.length
			}
		}
		// Check if all values are received.
		st.current = st.current.nextResultSet()
		if st.current == nil {
			st.request.responses <- &responseEnv{st.result, nil}
			return &idlingState{}, nil
		}
	}
	return st, nil
}

//--------------------
// SUBSCRIPTION STATE
//--------------------

// subscriptionState represents a connector waiting for the
// responses after sending any of the subscription commands.
type subscriptionState struct {
	request *requestEnv
	result  *ResultSet
	current *ResultSet
}

func newSubscriptionState(request *requestEnv, allTodo int) *subscriptionState {
	st := &subscriptionState{
		request: request,
		result:  newResultSet(),
	}
	st.current = st.result
	return st
}

func (st *subscriptionState) handle(conn *Connection, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// No request expected here.
		return nil, errors.New(ErrUnexpectedRequest, errorMessages, request)
	case reply != nil:
		// Handle reply.
		switch reply.kind {
		case receivingError:
			st.request.responses <- &responseEnv{nil, reply.err}
			return &idlingState{}, nil
		case timeoutError:
			st.request.responses <- &responseEnv{nil, errors.New(ErrTimeout, errorMessages, st.request.command)}
			return &idlingState{}, nil
		case errorReply:
			st.request.responses <- &responseEnv{nil, errors.New(ErrServerResponse, errorMessages, reply.value())}
			return &idlingState{}, nil
		case statusReply, integerReply, bulkReply, nullBulkReply:
			st.current.append(reply.value())
		case arrayReply:
			switch {
			case st.current == st.result && st.current.Len() == 0:
				st.current.length = reply.length
			case !st.current.allReceived():
				next := newResultSet()
				next.parent = st.current
				st.current.append(next)
				st.current = next
				st.current.length = reply.length
			}
		}
		// Check if all values are received.
		st.current = st.current.nextResultSet()
		if st.current == nil {
			st.request.responses <- &responseEnv{st.result, nil}
			return newSsubscribedState(st.request), nil
		}
	}
	return st, nil
}

//--------------------
// SUBSCRIBED STATE
//--------------------

// subscribedState represents a connector waiting for publishings or commands
// changing the subscription.
type subscribedState struct {
	request *requestEnv
	result  *ResultSet
	current *ResultSet
}

func newSsubscribedState(request *requestEnv) *subscribedState {
	st := &subscribedState{
		request: request,
		result:  newResultSet(),
	}
	st.current = st.result
	return st
}

func (st *subscribedState) handle(conn *Connection, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// Subscription changing requests are valid.
		if strings.Contains(request.command, "subscribe") {
			// Anyone of the (un-)subscribe commands.
			if err := conn.sendCommand(request.command, request.arguments); err != nil {
				return nil, err
			}
			return newSubscriptionState(request, len(request.arguments)), nil
		}
		// No other request expected here.
		return nil, errors.New(ErrUnexpectedRequest, errorMessages, request)
	case reply != nil:
		// Handle reply.
		switch reply.kind {
		case receivingError:
			logger.Errorf("error receiving publishing: %v", reply.err)
			return &idlingState{}, nil
		case errorReply:
			st.request.responses <- &responseEnv{nil, errors.New(ErrServerResponse, errorMessages, reply.value())}
			return &idlingState{}, nil
		case statusReply, integerReply, bulkReply, nullBulkReply:
			st.current.append(reply.value())
		case arrayReply:
			switch {
			case st.current == st.result && st.current.Len() == 0:
				st.current.length = reply.length
			case !st.current.allReceived():
				next := newResultSet()
				next.parent = st.current
				st.current.append(next)
				st.current = next
				st.current.length = reply.length
			}
		}
		// Check if all values are received.
		st.current = st.current.nextResultSet()
		if st.current == nil {
			published, err := newPublishedValue(st.result)
			if err != nil {
				logger.Errorf("error in published values: %s", err.Error())
				return st, nil
			}
			select {
			case st.request.publishings <- published:
				// Ok.
				st.result = newResultSet()
			default:
				// Not sent, channel is closed and connector not
				// needed anymore.
				return nil, nil
			}
		}
	}
	return st, nil
}

// EOF
