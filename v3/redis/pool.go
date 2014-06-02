// Tideland Go Data Management - Redis Client - resp Pool
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

// pool manages a number of Redis resp instances.
type pool struct {
	mux       sync.Mutex
	database  *Database
	available map[*resp]*resp
	inUse     map[*resp]*resp
}

// newPool creates a connection pool with uninitialized
// protocol instances.
func newPool(db *Database) *pool {
	return &pool{
		database:  db,
		available: make(map[*resp]*resp),
		inUse:     make(map[*resp]*resp),
	}
}

// close closes all pooled protocol instances, first the available ones,
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

// pull returns a protocol out of the pool. If none is available
// but the configured pool sized isn't reached a new one will be
// established.
func (p *pool) pull(forced bool) (*resp, error) {
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
		resp, err := newResp(p.database)
		if err != nil {
			return nil, err
		}
		p.inUse[resp] = resp
		return resp, nil
	}
	return nil, errors.New(ErrPoolLimitReached, errorMessages, p.database.poolsize)
}

// push returns a protocol back into the pool.
func (p *pool) push(resp *resp) error {
	p.mux.Lock()
	defer p.mux.Unlock()
	delete(p.inUse, resp)
	if len(p.available) < p.database.poolsize {
		p.available[resp] = resp
		return nil
	}
	return resp.close()
}

// kill closes the connection and removes it from the pool.
func (p *pool) kill(resp *resp) error {
	p.mux.Lock()
	defer p.mux.Unlock()
	delete(p.inUse, resp)
	return resp.close()
}

// EOF
