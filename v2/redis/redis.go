// Tideland Go Data Management - Redis Client
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
	"fmt"
	"sync"
	"time"

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// CONFIGURATION
//--------------------

// Configuration of a database client.
type Configuration struct {
	// Address specifies the IP address and port or the
	// file name of the Unix socket to connect Redis.
	// Default is "127.0.0.1:6379".
	Address string

	// UnixSockets has to be set to true if a Unix socket
	// shall be used for connection.
	UnixSockets bool

	// Timeout is the dialing timeout to connect Redis.
	// Default are 5 seconds.
	Timeout time.Duration

	// Database specifies the database number to connect.
	// Default is 0.
	Database int

	// Auth has to be set to the authentication password
	// if the server is protected. An unset field leads
	// to no authentication.
	Auth string

	// PoolSize defines the number of connectors pooled
	// for one connected database. Default is 10.
	PoolSize int

	// LogCommands has to be set to true if all commands
	// shall be logged with info level.
	LogCommands bool

	// MonitorCommands has to be set to true if the
	// command execution shall be monitored using the
	// GOAS monitoring package.
	MonitorCommands bool
}

// String returns the configured address and
// database as string.
func (c *Configuration) String() string {
	return fmt.Sprintf("Redis connection to %d on %s", c.Database, c.Address)
}

// check validates the configuration and may change unset
// options to default values.
func (c *Configuration) check() error {
	if c.Address == "" {
		c.Address = "127.0.0.1:6379"
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	} else if c.Timeout < 0 {
		return errors.New(ErrInvalidConfiguration, errorMessages, "timeout", c.Timeout)
	}
	if c.Database < 0 {
		return errors.New(ErrInvalidConfiguration, errorMessages, "database", c.Database)
	}
	if c.PoolSize == 0 {
		c.PoolSize = 10
	} else if c.PoolSize < 0 {
		return errors.New(ErrInvalidConfiguration, errorMessages, "pool size", c.PoolSize)
	}
	return nil
}

//--------------------
// DATABASE
//--------------------

// Database manages the access to one Redis database.
type Database interface {
	// Close closes the database.
	Close()

	// Command performs a Redis command.
	Command(cmd string, args ...interface{}) (ResultSet, error)

	// AsyncCommand performs a Redis command asynchronously.
	AsyncCommand(cmd string, args ...interface{}) Future

	// MultiCommand executes a function for the performing
	// of multiple commands in one call.
	MultiCommand(f func(MultiCommand) error) (ResultSets, error)

	// AsyncMultiCommand executes a function for the performing
	// of multiple commands in one call asynchronously.
	AsyncMultiCommand(f func(MultiCommand) error) Future

	// Subscribe to one or more channels.
	Subscribe(channels ...string) (Subscription, error)

	// Publish a message to a channel.
	Publish(channel string, message interface{}) (int, error)
}

// database implements the Database interface.
type database struct {
	mux           sync.Mutex
	configuration *Configuration
	pool          chan *connector
	connectorId   int
}

// Connect connects a Redis database based on the configuration.
func Connect(c *Configuration) (Database, error) {
	var cc *Configuration
	if c == nil {
		cc = &Configuration{}
	} else {
		tmp := *c
		cc = &tmp
	}
	if err := cc.check(); err != nil {
		return nil, err
	}
	return &database{
		configuration: cc,
		pool:          make(chan *connector, cc.PoolSize),
	}, nil
}

func (db *database) Close() {
	db.mux.Lock()
	defer db.mux.Unlock()

	db.pool = nil
}

func (db *database) Command(cmd string, args ...interface{}) (ResultSet, error) {
	conn, err := db.pullConnector()
	if err != nil {
		return nil, err
	}
	defer db.pushConnector(conn)
	return conn.command(cmd, args...)
}

func (db *database) AsyncCommand(cmd string, args ...interface{}) Future {
	fut := newFuture()
	go func() {
		rs, err := db.Command(cmd, args...)
		fut.setResult(rs, err)
	}()
	return fut
}

func (db *database) MultiCommand(f func(MultiCommand) error) (ResultSets, error) {
	cnctr, err := db.pullConnector()
	if err != nil {
		return nil, err
	}
	defer db.pushConnector(cnctr)
	return cnctr.multiCommand(f)
}

func (db *database) AsyncMultiCommand(f func(MultiCommand) error) Future {
	fut := newFuture()
	go func() {
		rss, err := db.MultiCommand(f)
		fut.setResult(rss, err)
	}()
	return fut
}

func (db *database) Subscribe(channels ...string) (Subscription, error) {
	cnctr, err := db.pullConnector()
	if err != nil {
		return nil, err
	}
	return newSubscription(cnctr, channels...)
}

func (db *database) Publish(channel string, message interface{}) (int, error) {
	rs, err := db.Command("publish", channel, message)
	if err != nil {
		return 0, err
	}
	return rs.FirstValue().Int()
}

// pullConnector pulls a connector out of the pool.
func (db *database) pullConnector() (*connector, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	if db.pool == nil {
		return nil, errors.New(ErrDatabaseClosed, errorMessages)
	}

	select {
	case cnctr := <-db.pool:
		return cnctr, nil
	default:
		db.connectorId++
		return connect(db.connectorId, db.configuration)
	}
}

// pushConnector pushes a connector back into the pool.
func (db *database) pushConnector(cnctr *connector) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	if db.pool == nil {
		return cnctr.stop()
	}

	select {
	case db.pool <- cnctr:
		// Everything ok.
		return nil
	default:
		// Pool is full, close connection.
		return cnctr.stop()
	}
}

// EOF
