// Tideland Go Data Management - Redis Client - Unit Tests - String Commands
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
	"time"

	"github.com/tideland/goas/v3/errors"
	"github.com/tideland/godm/v3/redis"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestSetGetSimple(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	// Not existing key.
	value, err := conn.Get("str:setget")
	assert.Nil(err)
	assert.Nil(value)

	// Simple values.
	err = conn.Set("str:setget", "foo")
	assert.Nil(err)
	value, err = conn.Get("str:setget")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")

	err = conn.SetNX("str:setget", "bar")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
}

func TestSetGetTTL(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	err := conn.SetEx("str:setget:ttl", time.Second, "bar")
	assert.Nil(err)
	value, err := conn.Get("str:setget:ttl")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	time.Sleep(2 * time.Second)
	exists, err := conn.Exists("str:setget:ttl")
	assert.Nil(err)
	assert.False(exists)

	err = conn.PSetEx("str:setget:ttl", 250*time.Millisecond, "foo")
	assert.Nil(err)
	value, err = conn.Get("str:setget:ttl")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	time.Sleep(750 * time.Millisecond)
	exists, err = conn.Exists("str:setget:ttl")
	assert.Nil(err)
	assert.False(exists)
}

func TestSetGetExists(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.Del("str:setget:exists")
	err := conn.SetExExists("str:setget:exists", time.Second, true, "foo")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
	err = conn.Set("str:setget:exists", "bar")
	assert.Nil(err)
	err = conn.SetExExists("str:setget:exists", time.Second, true, "foo")
	assert.Nil(err)
	value, err := conn.Get("str:setget:exists")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	time.Sleep(2 * time.Second)
	exists, err := conn.Exists("str:setget:exists")
	assert.Nil(err)
	assert.False(exists)
	err = conn.Set("str:setget:exists", "bar")
	assert.Nil(err)
	err = conn.SetExExists("str:setget:exists", time.Second, false, "foo")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))

	conn.Del("str:setget:exists")
	err = conn.PSetExExists("str:setget:exists", 250*time.Millisecond, true, "bar")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
	err = conn.Set("str:setget:exists", "foo")
	assert.Nil(err)
	err = conn.PSetExExists("str:setget:exists", 250*time.Millisecond, true, "bar")
	assert.Nil(err)
	value, err = conn.Get("str:setget:exists")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	time.Sleep(750 * time.Millisecond)
	exists, err = conn.Exists("str:setget:exists")
	assert.Nil(err)
	assert.False(exists)
	err = conn.Set("str:setget:exists", "foo")
	assert.Nil(err)
	err = conn.PSetExExists("str:setget:exists", 250*time.Millisecond, false, "bar")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
}

func TestMSetGet(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	// Simple setting and getting.
	h := redis.NewHash().Set("str:msetget:a", "foo").Set("str:msetget:b", 4711)
	err := conn.MSet(h)
	assert.Nil(err)
	result, err := conn.MGet("str:msetget:a", "str:msetget:b", "str:msetget:c")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "foo")
	assertEqualInt(assert, result, 1, 4711)
	assertNil(assert, result, 2)

	// Setting with existence check.
	h = redis.NewHash().Set("str:msetget:c", true).Set("str:msetget:d", "bar")
	err = conn.MSetNX(h)
	result, err = conn.MGet("str:msetget:c", "str:msetget:d")
	assert.Nil(err)
	assertEqualBool(assert, result, 0, true)
	assertEqualString(assert, result, 1, "bar")
	h = redis.NewHash().Set("str:msetget:d", "yadda").Set("str:msetget:e", false)
	err = conn.MSetNX(h)
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
}

func TestAppendRange(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	count, err := conn.Append("str:append", "foo")
	assert.Nil(err)
	assert.Equal(count, 3)
	count, err = conn.Append("str:append", "bar")
	assert.Nil(err)
	assert.Equal(count, 6)
	count, err = conn.Append("str:append", "yadda")
	assert.Nil(err)
	assert.Equal(count, 11)

	value, err := conn.Get("str:append")
	assert.Nil(err)
	assert.Equal(value.String(), "foobaryadda")

	length, err := conn.SetRange("str:append", 3, " / ")
	assert.Nil(err)
	assert.Equal(length, 11)
	value, err = conn.Get("str:append")
	assert.Nil(err)
	assert.Equal(value.String(), "foo / yadda")
	length, err = conn.StrLen("str:append")
	assert.Nil(err)
	assert.Equal(length, 11)
	value, err = conn.GetRange("str:append", 3, 5)
	assert.Nil(err)
	assert.Equal(value.String(), " / ")
}

func TestBit(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	count, err := conn.BitCount("str:bit")
	assert.Nil(err)
	assert.Equal(count, 0)

	conn.Set("str:bit", "UU")
	count, err = conn.BitCount("str:bit")
	assert.Nil(err)
	assert.Equal(count, 8)
	count, err = conn.BitCountInterval("str:bit", 1, 1)
	assert.Nil(err)
	assert.Equal(count, 4)

	conn.Set("str:bit:a", "U")
	conn.Set("str:bit:b", "<")
	conn.Set("str:bit:c", "X")
	size, err := conn.BitOp(redis.BitOpAnd, "str:bit:and", "str:bit:a", "str:bit:b")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("str:bit:and")
	assert.Nil(err)
	assert.Equal(count, 2)
	size, err = conn.BitOp(redis.BitOpOr, "str:bit:or", "str:bit:a", "str:bit:b")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("str:bit:or")
	assert.Nil(err)
	assert.Equal(count, 6)
	size, err = conn.BitOp(redis.BitOpXOr, "str:bit:xor", "str:bit:a", "str:bit:b")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("str:bit:xor")
	assert.Nil(err)
	assert.Equal(count, 4)
	size, err = conn.BitOp(redis.BitOpNot, "str:bit:not", "str:bit:c")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("str:bit:not")
	assert.Nil(err)
	assert.Equal(count, 5)
	bit, err := conn.GetBit("str:bit:a", 0)
	assert.Nil(err)
	assert.False(bit)
	bit, err = conn.GetBit("str:bit:a", 1)
	assert.Nil(err)
	assert.True(bit)
}

func TestIncrDecr(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	ival, err := conn.Incr("str:incr:a")
	assert.Nil(err)
	assert.Equal(ival, 1)
	ival, err = conn.Incr("str:incr:a")
	assert.Nil(err)
	assert.Equal(ival, 2)
	ival, err = conn.IncrBy("str:incr:a", 3)
	assert.Nil(err)
	assert.Equal(ival, 5)
	ival, err = conn.IncrBy("str:incr:a", -5)
	assert.Nil(err)
	assert.Equal(ival, 0)
	ival, err = conn.Decr("str:incr:a")
	assert.Nil(err)
	assert.Equal(ival, -1)
	ival, err = conn.DecrBy("str:incr:a", 4)
	assert.Nil(err)
	assert.Equal(ival, -5)

	err = conn.Set("str:incr:b", 8.15)
	assert.Nil(err)
	fval, err := conn.IncrByFloat("str:incr:b", 47.11)
	assert.Nil(err)
	assert.Equal(fval, 55.26)

	err = conn.Set("str:incr:c", "no number")
	assert.Nil(err)
	ival, err = conn.Incr("str:incr:c")
	assert.True(errors.IsError(err, redis.ErrServerResponse))
}

// EOF
