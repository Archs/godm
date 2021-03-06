// Tideland Go Data Management - Redis Client - Unit Tests
//
// Copyright (C) 2009-2013 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis_test

//--------------------
// IMPORTS
//--------------------

import (
	"testing"

	"github.com/tideland/goas/v2/logger"
	"github.com/tideland/godm/v3/redis"
	"github.com/tideland/gots/V3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestUnixSocketConnection(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert, redis.UnixConnection("", 0))
	defer restore()

	result, err := conn.Do("echo", "Hello, World!")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "Hello, World!")
	result, err = conn.Do("ping")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "+PONG")
}

func BenchmarkUnixConnection(b *testing.B) {
	assert := asserts.NewTestingAssertion(b, true)
	conn, restore := connectDatabase(assert, redis.UnixConnection("", 0))
	defer restore()

	for i := 0; i < b.N; i++ {
		result, err := conn.Do("ping")
		assert.Nil(err)
		assertEqualString(assert, result, 0, "+PONG")
	}
}

func TestTcpConnection(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert, redis.TcpConnection("", 0))
	defer restore()

	result, err := conn.Do("echo", "Hello, World!")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "Hello, World!")
	result, err = conn.Do("ping")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "+PONG")
}

func BenchmarkTcpConnection(b *testing.B) {
	assert := asserts.NewTestingAssertion(b, true)
	conn, restore := connectDatabase(assert, redis.TcpConnection("", 0))
	defer restore()

	for i := 0; i < b.N; i++ {
		result, err := conn.Do("ping")
		assert.Nil(err)
		assertEqualString(assert, result, 0, "+PONG")
	}
}

func TestPipelining(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	ppl, restore := pipelineDatabase(assert)
	defer restore()

	for i := 0; i < 1000; i++ {
		err := ppl.Do("ping")
		assert.Nil(err)
	}

	results, err := ppl.Collect()
	assert.Nil(err)
	assert.Length(results, 1000)

	for _, result := range results {
		assertEqualString(assert, result, 0, "+PONG")
	}
}

func BenchmarkPipelining(b *testing.B) {
	assert := asserts.NewTestingAssertion(b, true)
	ppl, restore := pipelineDatabase(assert)
	defer restore()

	for i := 0; i < b.N; i++ {
		err := ppl.Do("ping")
		assert.Nil(err)
	}
	results, err := ppl.Collect()
	assert.Nil(err)
	assert.Length(results, b.N)

	for _, result := range results {
		assertEqualString(assert, result, 0, "+PONG")
	}
}

//--------------------
// TOOLS
//--------------------

func init() {
	logger.SetLevel(logger.LevelDebug)
}

// testDatabaseIndex defines the database index for the tests to not
// get in conflict with existing databases.
const testDatabaseIndex = 99

// connectDatabase connects to a Redis database with the given options
// and returns a connection and a function for closing. This function
// shall be called with defer.
func connectDatabase(assert asserts.Assertion, options ...redis.Option) (*redis.Connection, func()) {
	// Open and connect database.
	options = append(options, redis.Index(testDatabaseIndex, ""))
	db, err := redis.Open(options...)
	assert.Nil(err)
	conn, err := db.Connection()
	assert.Nil(err)
	// Flush all keys to get a clean testing environment.
	_, err = conn.Do("flushdb")
	assert.Nil(err)
	// Return connection and cleanup function.
	return conn, func() {
		conn.Return()
		db.Close()
	}
}

// pipelineDatabase connects to a Redis database with the given options
// and returns a pipeling and a function for closing. This function
// shall be called with a defer.
func pipelineDatabase(assert asserts.Assertion, options ...redis.Option) (*redis.Pipeline, func()) {
	// Open and connect database.
	options = append(options, redis.Index(testDatabaseIndex, ""))
	db, err := redis.Open(options...)
	assert.Nil(err)
	ppl, err := db.Pipeline()
	assert.Nil(err)
	// Return pipeline and cleanup function.
	return ppl, func() {
		db.Close()
	}
}

// subscribeDatabase connects to a Redis database with the given options
// and returns a subscription and a function for closing. This function
// shall be called with a defer.
func subscribeDatabase(assert asserts.Assertion, options ...redis.Option) (*redis.Subscription, func()) {
	// Open and connect database.
	options = append(options, redis.Index(testDatabaseIndex, ""))
	db, err := redis.Open(options...)
	assert.Nil(err)
	sub, err := db.Subscription()
	assert.Nil(err)
	// Return subscription and cleanup function.
	return sub, func() {
		sub.Close()
		db.Close()
	}
}

// assertEqualString checks if the result at index is value.
func assertEqualString(assert asserts.Assertion, result *redis.ResultSet, index int, value string) {
	s, err := result.StringAt(index)
	assert.Nil(err)
	assert.Equal(s, value)
}

// assertEqualBool checks if the result at index is value.
func assertEqualBool(assert asserts.Assertion, result *redis.ResultSet, index int, value bool) {
	b, err := result.BoolAt(index)
	assert.Nil(err)
	assert.Equal(b, value)
}

// assertEqualInt checks if the result at index is value.
func assertEqualInt(assert asserts.Assertion, result *redis.ResultSet, index, value int) {
	i, err := result.IntAt(index)
	assert.Nil(err)
	assert.Equal(i, value)
}

// assertNil checks if the result at index is nil.
func assertNil(assert asserts.Assertion, result *redis.ResultSet, index int) {
	v, err := result.ValueAt(index)
	assert.Nil(err)
	assert.Nil(v)
}

// EOF
