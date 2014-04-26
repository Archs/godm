// Tideland Go Data Management - Redis Client - Connection Pool
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

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// CONNECTION POOL
//--------------------

const (
	forcedPull   = true
	unforcedPull = false
)

// pool manages a number of Redis connections.
type pool struct {
	mux       sync.Mutex
	database  *Database
	available map[*Connection]*Connection
	inUse     map[*Connection]*Connection
}

// newPool creates a connection pool with uninitialized
// connections.
func newPool(db *Database) *pool {
	return &pool{
		database:  db,
		available: make(map[*Connection]*Connection),
		inUse:     make(map[*Connection]*Connection),
	}
}

// close closes all pooled connections, first the available ones,
// then the ones in use.
func (p *pool) close() error {
	p.mux.Lock()
	defer p.mux.Unlock()
	for conn := range p.available {
		if err := conn.close(); err != nil {
			return err
		}
	}
	for conn := range p.inUse {
		if err := conn.close(); err != nil {
			return err
		}
	}
	return nil
}

// pull returns a connection out of the pool. If none is available
// but the configured pool sized isn't reached a new one will be
// established.
func (p *pool) pull(forced bool) (*Connection, error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	// Check if connections are available.
	if len(p.available) > 0 {
		for conn := range p.available {
			p.inUse[conn] = conn
			return conn, nil
		}
	}
	// No connection available, so create a new one if not all
	// in use or the creation is forced.
	if len(p.inUse) < p.database.poolsize || forced {
		conn, err := connect(p.database)
		if err != nil {
			return nil, err
		}
		p.inUse[conn] = conn
		return conn, nil
	}
	return nil, errors.New(ErrCannotRenameKey, errorMessages, p.database.poolsize)
}

// push returns a connection back into the pool.
func (p *pool) push(conn *Connection) error {
	p.mux.Lock()
	defer p.mux.Unlock()
	// Remove from set of used connections.
	delete(p.inUse, conn)
	// Check if pool is full.
	if len(p.available) < p.database.poolsize {
		p.available[conn] = conn
		return nil
	}
	// Close connection.
	return conn.close()
}

// EOF
