// Tideland Go Data Management - Redis Client - Connector
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
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/tideland/goas/v2/identifier"
	"github.com/tideland/goas/v2/logger"
	"github.com/tideland/goas/v2/loop"
	"github.com/tideland/goas/v2/monitoring"
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// ENVELOPES
//--------------------

// responseEnv encapsulates the response to the caller
// of a command or a subscriber.
type responseEnv struct {
	rs     ResultSet
	rss    ResultSets
	number int
	err    error
}

// requestEnv encapsulates a request for the Redis server.
type requestEnv struct {
	command     string
	arguments   []interface{}
	responses   chan *responseEnv
	publishings chan PublishedValue
}

// String creates a string representation of the request.
func (r *requestEnv) String() string {
	return fmt.Sprintf("REQUEST (C: %s A: %v)", r.command, r.arguments)
}

//--------------------
// STATES
//--------------------

// state represents a connector state and is responsible
// for handling a response.
type state interface {
	// handle handles a request or reply and returns the next state.
	handle(cnctr *connector, request *requestEnv, reply *replyEnv) (state, error)
}

// idlingState represents an idling connector.
type idlingState struct{}

func (st *idlingState) handle(cnctr *connector, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// Send command to the server.
		if err := cnctr.sendCommand(request.command, request.arguments); err != nil {
			return nil, err
		}
		if strings.Contains(request.command, "subscribe") {
			// Anyone of the (un-)subscribe commands.
			return newSubscriptionState(request, len(request.arguments)), nil
		}
		if request.command == "exec" {
			// A multi-command is executed.
			return newExecState(request), nil
		}
		// A regular command.
		return newCommandState(request), nil
	case reply != nil:
		// No reply expected here.
		return nil, errors.New(ErrUnexpectedReply, errorMessages, reply)
	}
	return st, nil
}

// commandState represents a connector waiting for a
// single response after sending a command.
type commandState struct {
	request   *requestEnv
	resultSet ResultSet
	rsTodo    int
}

func newCommandState(request *requestEnv) *commandState {
	return &commandState{
		request:   request,
		resultSet: ResultSet{},
		rsTodo:    1,
	}
}

func (st *commandState) handle(cnctr *connector, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// No request expected here.
		return nil, errors.New(ErrUnexpectedRequest, errorMessages, request)
	case reply != nil:
		// Handle reply.
		switch reply.kind {
		case receivingError, nullBulkReply:
			st.request.responses <- &responseEnv{nil, nil, 0, reply.err}
			return &idlingState{}, nil
		case timeoutError:
			st.request.responses <- &responseEnv{nil, nil, 0, errors.New(ErrTimeout, errorMessages, st.request.command)}
			return &idlingState{}, nil
		case statusReply, errorReply, integerReply, bulkReply:
			st.resultSet = append(st.resultSet, reply.value())
		case multiBulkReply:
			count, err := reply.int()
			if err != nil {
				st.request.responses <- &responseEnv{nil, nil, 0, err}
				return &idlingState{}, nil
			}
			st.rsTodo = count
		}
		// Check if all values are received.
		if len(st.resultSet) == st.rsTodo {
			st.request.responses <- &responseEnv{st.resultSet, nil, 0, nil}
			return &idlingState{}, nil
		}
	}
	return st, nil
}

// execState represents a connector waiting for a
// all responses after sending an "exec" command.
type execState struct {
	request    *requestEnv
	resultSets ResultSets
	resultSet  ResultSet
	rssTodo    int
	rsTodo     int
}

func newExecState(request *requestEnv) *execState {
	return &execState{
		request:    request,
		resultSets: ResultSets{},
		resultSet:  ResultSet{},
		rssTodo:    -1,
		rsTodo:     1,
	}
}

func (st *execState) handle(cnctr *connector, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// No request expected here.
		return nil, errors.New(ErrUnexpectedRequest, errorMessages, request)
	case reply != nil:
		// Handle reply.
		var count int = -1
		var err error
		switch reply.kind {
		case receivingError, nullBulkReply:
			st.request.responses <- &responseEnv{nil, nil, 0, reply.err}
			return &idlingState{}, nil
		case timeoutError:
			st.request.responses <- &responseEnv{nil, nil, 0, errors.New(ErrTimeout, errorMessages, st.request.command)}
			return &idlingState{}, nil
		case statusReply, errorReply, integerReply, bulkReply:
			st.resultSet = append(st.resultSet, reply.value())
		case multiBulkReply:
			count, err = reply.int()
			if err != nil {
				st.request.responses <- &responseEnv{nil, nil, 0, err}
				return &idlingState{}, nil
			}
		}
		// Check state for next actions.
		switch {
		case st.rssTodo == -1:
			// First length of all result sets.
			st.rssTodo = count
		case len(st.resultSet) == st.rsTodo:
			//  Full result set, append to result sets.
			st.resultSets = append(st.resultSets, st.resultSet)
			st.resultSet = ResultSet{}
			st.rsTodo = 1
		case count != -1:
			// Length of next result set.
			st.rsTodo = count
		}
		// Check if it's time to leave.
		if len(st.resultSets) == st.rssTodo {
			st.request.responses <- &responseEnv{nil, st.resultSets, 0, nil}
			return &idlingState{}, nil
		}
	}
	return st, nil
}

// subscriptionState represents a connector waiting for the
// responses after sending any of the subscription commands.
type subscriptionState struct {
	request    *requestEnv
	resultSets ResultSets
	resultSet  ResultSet
	rssTodo    int
	rsTodo     int
}

func newSubscriptionState(request *requestEnv, rssTodo int) *subscriptionState {
	return &subscriptionState{
		request:    request,
		resultSets: ResultSets{},
		resultSet:  ResultSet{},
		rssTodo:    rssTodo,
		rsTodo:     -1,
	}
}

func (st *subscriptionState) handle(cnctr *connector, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// No request expected here.
		return nil, errors.New(ErrUnexpectedRequest, errorMessages, request)
	case reply != nil:
		// Handle reply.
		var count int = -1
		var err error
		switch reply.kind {
		case receivingError, nullBulkReply:
			st.request.responses <- &responseEnv{nil, nil, 0, reply.err}
			return &idlingState{}, nil
		case timeoutError:
			st.request.responses <- &responseEnv{nil, nil, 0, errors.New(ErrTimeout, errorMessages, st.request.command)}
			return &idlingState{}, nil
		case statusReply, errorReply, integerReply, bulkReply:
			st.resultSet = append(st.resultSet, reply.value())
		case multiBulkReply:
			count, err = reply.int()
			if err != nil {
				st.request.responses <- &responseEnv{nil, nil, 0, err}
				return &idlingState{}, nil
			}
		}
		// Check state for next actions.
		switch {
		case len(st.resultSet) == st.rsTodo:
			//  Full result set, append to result sets.
			st.resultSets = append(st.resultSets, st.resultSet)
			st.resultSet = ResultSet{}
			st.rsTodo = -1
		case count != -1:
			// Length of next result set.
			st.rsTodo = count
		}
		// Check if it's time to leave.
		if len(st.resultSets) == st.rssTodo {
			count, err = st.resultSets[st.rssTodo-1][2].Int()
			if err != nil {
				st.request.responses <- &responseEnv{nil, nil, 0, err}
				return &idlingState{}, nil
			}
			st.request.responses <- &responseEnv{nil, nil, count, nil}
			return newSsubscribedState(st.request), nil
		}
	}
	return st, nil
}

// subscribedState represents a connector waiting for publishings or commands
// changing the subscription.
type subscribedState struct {
	request   *requestEnv
	resultSet ResultSet
	rsTodo    int
}

func newSsubscribedState(request *requestEnv) *subscribedState {
	return &subscribedState{
		request:   request,
		resultSet: ResultSet{},
		rsTodo:    -1,
	}
}

func (st *subscribedState) handle(cnctr *connector, request *requestEnv, reply *replyEnv) (state, error) {
	switch {
	case request != nil:
		// Subscription changing requests are valid.
		if strings.Contains(request.command, "subscribe") {
			// Anyone of the (un-)subscribe commands.
			if err := cnctr.sendCommand(request.command, request.arguments); err != nil {
				return nil, err
			}
			return newSubscriptionState(request, len(request.arguments)), nil
		}
		// No other request expected here.
		return nil, errors.New(ErrUnexpectedRequest, errorMessages, request)
	case reply != nil:
		// Handle reply.
		switch reply.kind {
		case receivingError, nullBulkReply:
			logger.Errorf("error receiving publishing: %v", reply.err)
			return &idlingState{}, nil
		case statusReply, errorReply, integerReply, bulkReply:
			st.resultSet = append(st.resultSet, reply.value())
		case multiBulkReply:
			count, err := reply.int()
			if err != nil {
				logger.Errorf("error receiving publishing: %v", reply.err)
				return &idlingState{}, nil
			}
			st.rsTodo = count
		}
		if len(st.resultSet) == st.rsTodo {
			var published *publishedValue
			switch len(st.resultSet) {
			case 3:
				published = &publishedValue{st.resultSet[2], "*", st.resultSet[1].String()}
			case 4:
				published = &publishedValue{st.resultSet[3], st.resultSet[1].String(), st.resultSet[2].String()}
			default:
				logger.Errorf("error in published values: %v", st.resultSet)
				return st, nil
			}
			select {
			case st.request.publishings <- published:
				// Ok.
				st.resultSet = ResultSet{}
				st.rsTodo = -1
			default:
				// Not sent, channel is closed and connector not
				// needed anymore.
				return nil, nil
			}
		}
	}
	return st, nil
}

//--------------------
// CONNECTOR
//--------------------

// connector manages one connector to a database.
type connector struct {
	configuration *Configuration
	id            int
	state         state
	conn          net.Conn
	requests      chan *requestEnv
	receiver      *receiver
	writer        *bufio.Writer
	loop          loop.Loop
}

// connect establishes a connector to a database based
// on the passed configuration.
func connect(id int, cfg *Configuration) (*connector, error) {
	// Establish the connector.
	network := "tcp"
	if cfg.UnixSockets {
		// Use faster unix sockets.
		network = "unix"
	}
	conn, err := net.DialTimeout(network, cfg.Address, cfg.Timeout)
	if err != nil {
		return nil, errors.Annotate(err, ErrConnection, errorMessages)
	}
	// Create the connector instance and start
	// its backend loop and receiver.
	cnctr := &connector{
		configuration: cfg,
		id:            id,
		state:         &idlingState{},
		conn:          conn,
		requests:      make(chan *requestEnv),
		receiver:      newReceiver(id, conn),
		writer:        bufio.NewWriter(conn),
	}
	cnctr.loop = loop.Go(cnctr.backendLoop)
	// Perform authentication and database selection.
	err = cnctr.authenticate()
	if err != nil {
		return nil, err
	}
	err = cnctr.selectDatabase()
	if err != nil {
		return nil, err
	}
	return cnctr, nil
}

// command executes one Redis command and returns
// the result as result set.
func (c *connector) command(cmd string, args ...interface{}) (rs ResultSet, err error) {
	cmd = strings.ToLower(cmd)
	defer logCommand(cmd, args, err, c.configuration.LogCommands)

	if c.configuration.MonitorCommands {
		m := monitoring.BeginMeasuring(identifier.Identifier("redis", "command", cmd))
		defer m.EndMeasuring()
	}

	request := &requestEnv{cmd, args, make(chan *responseEnv), nil}
	c.requests <- request
	response := <-request.responses
	return response.rs, response.err
}

// multiCommand executes a multi command function and returns
// the result as a slice of result sets.
func (c *connector) multiCommand(f func(MultiCommand) error) (rss ResultSets, err error) {
	mc, err := newMultiCommand(c)
	if err != nil {
		return nil, err
	}
	if err = f(mc); err != nil {
		return nil, err
	}
	defer logCommand("exec", nil, err, c.configuration.LogCommands)

	request := &requestEnv{"exec", nil, make(chan *responseEnv), nil}
	c.requests <- request
	response := <-request.responses
	return response.rss, response.err
}

// authenticate authenticates against the server if configured.
func (c *connector) authenticate() error {
	if c.configuration.Auth != "" {
		_, err := c.command("auth", c.configuration.Auth)
		if err != nil {
			c.stop()
			return err
		}
	}
	return nil
}

// selectDatabase selects the database
func (c *connector) selectDatabase() error {
	_, err := c.command("select", c.configuration.Database)
	if err != nil {
		c.stop()
		return err
	}
	return nil
}

// subscription subscribes or unsubscribes this connector to a number of
// channels and returns the number of currently subscribed channels.
func (c *connector) subscription(publishings chan PublishedValue, cmd string, channels ...string) (count int, err error) {
	cmd = strings.ToLower(cmd)
	for _, channel := range channels {
		if containsPattern(channel) {
			cmd = "p" + cmd
			break
		}
	}
	args := stringsToInterfaces(channels...)
	defer logCommand(cmd, args, err, c.configuration.LogCommands)

	if c.configuration.MonitorCommands {
		m := monitoring.BeginMeasuring(identifier.Identifier("redis", "command", cmd))
		defer m.EndMeasuring()
	}

	request := &requestEnv{cmd, args, make(chan *responseEnv), publishings}
	c.requests <- request
	response := <-request.responses
	if response.err != nil {
		return -1, response.err
	}
	return response.number, nil
}

// stop closes the connector to the database.
func (c *connector) stop() error {
	return c.loop.Stop()
}

// sendCommand builds and sends a command packet.
func (c *connector) sendCommand(cmd string, args []interface{}) error {
	lengthPart := buildLengthPart(args)
	cmdPart := buildValuePart(cmd)
	argsPart := buildArgumentsPart(args)

	packet := join(lengthPart, cmdPart, argsPart)
	_, err := c.writer.Write(packet)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// backendLoop manages the state and the communication
// of the connector.
func (c *connector) backendLoop(loop loop.Loop) error {
	defer c.receiver.stop()
	defer c.conn.Close()
	for {
		var request *requestEnv
		var reply *replyEnv
		select {
		case <-loop.ShallStop():
			return nil
		case request = <-c.requests:
		case reply = <-c.receiver.replies:
		}
		state, err := c.state.handle(c, request, reply)
		if err != nil {
			return err
		}
		if state == nil {
			return nil
		}
		c.state = state
	}
}

// EOF
