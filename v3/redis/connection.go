// Tideland Go Data Management - Redis Client - Connection
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
	"bufio"
	"net"
	"strings"
	"sync"

	"github.com/tideland/goas/v2/identifier"
	"github.com/tideland/goas/v2/loop"
	"github.com/tideland/goas/v2/monitoring"
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// CONNECTION
//--------------------

// Connection manages one connection to a Redis database.
type Connection struct {
	mux      sync.Mutex
	database *Database
	state    state
	redis    net.Conn
	requests chan *requestEnv
	receiver *receiver
	writer   *bufio.Writer
	loop     loop.Loop
}

// connect establishes a connection to a database based
// on the passed database.
func connect(db *Database) (*Connection, error) {
	// Establish the connection to Redis.
	redis, err := net.DialTimeout(db.network, db.address, db.timeout)
	if err != nil {
		return nil, errors.Annotate(err, ErrConnectionEstablishing, errorMessages)
	}
	// Create the connection instance and start
	// its backend loop and receiver.
	conn := &Connection{
		database: db,
		state:    &idlingState{},
		redis:    redis,
		requests: make(chan *requestEnv),
		receiver: newReceiver(redis),
		writer:   bufio.NewWriter(redis),
	}
	conn.loop = loop.Go(conn.backendLoop)
	// Perform authentication and database selection.
	err = conn.authenticate()
	if err != nil {
		return nil, err
	}
	err = conn.selectDatabase()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Command executes one Redis command and returns
// the result as result set.
func (conn *Connection) Command(cmd string, args ...interface{}) (*ResultSet, error) {
	cmd = strings.ToLower(cmd)
	if conn.database.monitoring {
		m := monitoring.BeginMeasuring(identifier.Identifier("redis", "command", cmd))
		defer m.EndMeasuring()
	}
	return conn.request(cmd, args, nil)
}

// Return passes the connection back into the database pool.
func (conn *Connection) Return() error {
	return conn.database.pool.push(conn)
}

// authenticate authenticates against the server if configured.
func (conn *Connection) authenticate() error {
	if conn.database.password != "" {
		_, err := conn.Command("auth", conn.database.password)
		if err != nil {
			conn.close()
			return err
		}
	}
	return nil
}

// selectDatabase selects the database.
func (conn *Connection) selectDatabase() error {
	_, err := conn.Command("select", conn.database.index)
	if err != nil {
		conn.close()
		return err
	}
	return nil
}

// sendCommand builds and sends a command packet.
func (conn *Connection) sendCommand(cmd string, args []interface{}) error {
	lengthPart := buildLengthPart(args)
	cmdPart := buildValuePart(cmd)
	argsPart := buildArgumentsPart(args)

	packet := join(lengthPart, cmdPart, argsPart)
	_, err := conn.writer.Write(packet)
	if err != nil {
		return err
	}
	return conn.writer.Flush()
}

// request sends a request to the backend.
func (conn *Connection) request(cmd string, args []interface{}, publishings chan *PublishedValue) (*ResultSet, error) {
	request := &requestEnv{cmd, args, make(chan *responseEnv), publishings}
	select {
	case conn.requests <- request:
	case <-conn.loop.IsStopping():
		_, err := conn.loop.Error()
		err = errors.Annotate(err, ErrConnectionClosed, errorMessages)
		logCommand(cmd, args, nil, err, conn.database.logging)
		return nil, err
	}
	select {
	case response := <-request.responses:
		logCommand(cmd, args, response.result, response.err, conn.database.logging)
		return response.result, response.err
	case <-conn.loop.IsStopping():
		_, err := conn.loop.Error()
		err = errors.Annotate(err, ErrConnectionClosed, errorMessages)
		logCommand(cmd, args, nil, err, conn.database.logging)
		return nil, err
	}
}

// close ends the connection to Redis.
func (conn *Connection) close() error {
	return conn.loop.Stop()
}

// backendLoop manages the state and the communication
// of the connector.
func (conn *Connection) backendLoop(loop loop.Loop) error {
	defer conn.receiver.stop()
	defer conn.redis.Close()
	for {
		var request *requestEnv
		var reply *replyEnv
		select {
		case <-loop.ShallStop():
			return nil
		case request = <-conn.requests:
		case reply = <-conn.receiver.replies:
		}
		state, err := conn.state.handle(conn, request, reply)
		if err != nil {
			return err
		}
		if state == nil {
			return nil
		}
		conn.state = state
	}
}

// EOF
