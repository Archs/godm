// Tideland Go Data Management - Redis Client - Commands - Transactions
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import ()

//--------------------
// TRANSACTION COMMANDS
//--------------------

// Multi marks the start of a transaction block.
func (conn *Connection) Multi() error {
	_, err := conn.Command("multi")
	return err
}

// Exec executes all previously queued commands in a
// transaction and restores the connection state to normal.
func (conn *Connection) Exec() (*ResultSet, error) {
	return conn.Command("exec")
}

// EOF
