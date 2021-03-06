// Tideland Go Data Management - Redis Client
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
	"sync"
	"time"
)

//--------------------
// DATABASE
//--------------------

// Database provides access to a Redis database.
type Database struct {
	mux        sync.Mutex
	address    string
	network    string
	timeout    time.Duration
	index      int
	password   string
	poolsize   int
	logging    bool
	monitoring bool
	pool       *pool
}

// Open opens the connection to a Redis database based on the
// passed options.
func Open(options ...Option) (*Database, error) {
	db := &Database{
		address:    defaultSocket,
		network:    defaultNetwork,
		timeout:    defaultTimeout,
		index:      defaultDatabase,
		password:   defaultPassword,
		poolsize:   defaultPoolSize,
		logging:    defaultLogging,
		monitoring: defaultMonitoring,
	}
	for _, option := range options {
		if err := option(db); err != nil {
			return nil, err
		}
	}
	db.pool = newPool(db)
	return db, nil
}

// Connection returns one of the pooled connections to the Redis
// server. It has to be returned with conn.Return() after usage.
func (db *Database) Connection() (*Connection, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	return newConnection(db)
}

// Pipeline returns one of the pooled connections to the Redis
// server running in pipeline mode. Calling ppl.Collect()
// collects all results and returns the connection.
func (db *Database) Pipeline() (*Pipeline, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	return newPipeline(db)
}

// Subscription returns a subscription with a connection to the
// Redis server. It has to be closed with sub.Close() after usage.
func (db *Database) Subscription() (*Subscription, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	return newSubscription(db)
}

// Close closes the database client.
func (db *Database) Close() error {
	db.mux.Lock()
	defer db.mux.Unlock()
	return db.pool.close()
}

// EOF
