// Tideland Go Data Management - Redis Client - Unit Tests - Transaction Commands
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis_test

//--------------------
// IMPORTS
//--------------------

import (
	"testing"

	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestMultiExec(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	err := conn.Multi()
	assert.Nil(err)
	conn.Set("tx:multi-exec:a", 1)
	conn.Set("tx:multi-exec:b", 2)
	conn.Set("tx:multi-exec:c", 3)
	conn.Del("tx:multi-exec:b")
	conn.Keys("tx:multi-exec:*")
	result, err := conn.Exec()
	assert.Nil(err)
	assert.Length(result, 5)
	value, err := result.ValueAt(0)
	assert.Nil(err)
	assert.Equal(value.String(), "OK")
	value, err = result.ValueAt(3)
	assert.Nil(err)
	assert.Equal(value.String(), "1")
	keys, err := result.ResultSetAt(4)
	assert.Nil(err)
	assert.Length(keys.Strings(), 2)
}

// EOF
