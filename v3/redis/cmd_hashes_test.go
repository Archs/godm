// Tideland Go Data Management - Redis Client - Unit Tests - Hash Commands
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
	"fmt"
	"testing"

	"github.com/tideland/goas/v3/errors"
	"github.com/tideland/godm/v3/redis"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestHSetGetDel(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	exists, err := conn.HExists("hashes:setget", "a")
	assert.Nil(err)
	assert.False(exists)
	added, err := conn.HSet("hashes:setget", "a", "foo")
	assert.Nil(err)
	assert.True(added)
	exists, err = conn.HExists("hashes:setget", "a")
	assert.Nil(err)
	assert.True(exists)
	added, err = conn.HSet("hashes:setget", "a", "bar")
	assert.Nil(err)
	assert.False(added)

	value, err := conn.HGet("hashes:setget", "a")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	value, err = conn.HGet("hashes:setget", "b")
	assert.Nil(err)
	assert.Nil(value)

	err = conn.HSetNX("hashes:setget", "a", "foo")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
	added, err = conn.HSet("hashes:setget", "b", "foo")
	assert.Nil(err)
	assert.True(added)

	count, err := conn.HDel("hashes:setget", "a", "b", "c", "d")
	assert.Nil(err)
	assert.Equal(count, 2)
}

func TestHIncr(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	added, err := conn.HSet("hashes:incr", "a", 5)
	assert.Nil(err)
	assert.True(added)
	ival, err := conn.HIncrBy("hashes:incr", "a", 5)
	assert.Nil(err)
	assert.Equal(ival, 10)
	added, err = conn.HSet("hashes:incr", "b", 5.5)
	assert.Nil(err)
	assert.True(added)
	fval, err := conn.HIncrByFloat("hashes:incr", "b", -1.1)
	assert.Nil(err)
	assert.Equal(fval, 4.4)

	added, err = conn.HSet("hashes:incr", "c", "no number")
	assert.Nil(err)
	assert.True(added)
	ival, err = conn.HIncrBy("hashes:incr", "c", 5)
	assert.True(errors.IsError(err, redis.ErrServerResponse))
}

func TestHMSetGet(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	h := redis.NewHash().Set("a", "foo").Set("b", 4711)
	err := conn.HMSet("hashes:msetget", h)
	assert.Nil(err)

	result, err := conn.HMGet("hashes:msetget", "a", "b", "c")
	assert.Nil(err)
	assert.Length(result, 3)
	sval, err := result.StringAt(0)
	assert.Nil(err)
	assert.Equal(sval, "foo")
	ival, err := result.IntAt(1)
	assert.Nil(err)
	assert.Equal(ival, 4711)
	value, err := result.ValueAt(2)
	assert.Nil(err)
	assert.Nil(value)
}

func TestHGetAll(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	h := redis.NewHash().Set("a", "foo").Set("b", 4711)
	err := conn.HMSet("hashes:getall", h)
	assert.Nil(err)

	kvs, err := conn.HGetAll("hashes:getall")
	assert.Nil(err)
	assert.Equal(kvs, h)
}

func TestHKeysVals(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	added, err := conn.HSet("hashes:keysvals", "a", "foo")
	assert.Nil(err)
	assert.True(added)
	added, err = conn.HSet("hashes:keysvals", "b", 4711)
	assert.Nil(err)
	assert.True(added)

	length, err := conn.HLen("hashes:keysvals")
	assert.Nil(err)
	assert.Equal(length, 2)

	keys, err := conn.HKeys("hashes:keysvals")
	assert.Nil(err)
	assert.Length(keys, 2)

	values, err := conn.HVals("hashes:keysvals")
	assert.Nil(err)
	assert.Length(values, 2)
}

func TestHScan(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateHashData(assert, conn, "hashes:scan", "abcdefghij")

	assertScan := func(pattern string, count, total int) {
		var cursor int
		var result redis.KeyValues
		var err error
		var max, all int

		if count == 0 {
			max = 20
		} else {
			max = count * 2
		}

		for {
			cursor, result, err = conn.HScan("hashes:scan", cursor, pattern, count)
			assert.Nil(err)
			all += result.Len()
			assert.True(result.Len() <= max)
			if cursor == 0 {
				break
			}
		}
		assert.Equal(all, total)
	}

	assertScan("", 0, 10000)
	assertScan("", 20, 10000)
	assertScan("field:a*", 0, 1000)
	assertScan("field:a*", 5, 1000)
	assertScan("-*-", 0, 0)
	assertScan("-*-", 20, 0)
}

//--------------------
// TOOLS
//--------------------

// generateHashData generates a hash of data at the given key.
func generateHashData(assert asserts.Assertion, conn *redis.Connection, key, charset string) {
	for i := 0; i < 1000; i++ {
		for _, c := range charset {
			field := fmt.Sprintf("field:%c%d", c, i)
			value := fmt.Sprintf("test data %c%d", c, i)
			_, err := conn.HSet(key, field, value)
			assert.Nil(err)
		}
	}
}

// EOF
