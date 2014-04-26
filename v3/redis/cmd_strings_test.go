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
	value, err := conn.Get("strings:setget")
	assert.Nil(err)
	assert.Nil(value)

	// Simple values.
	err = conn.Set("strings:setget", "foo")
	assert.Nil(err)
	value, err = conn.Get("strings:setget")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")

	err = conn.SetNX("strings:setget", "bar")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
}

func TestSetGetTTL(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	err := conn.SetEx("strings:setget:ttl", time.Second, "bar")
	assert.Nil(err)
	value, err := conn.Get("strings:setget:ttl")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	time.Sleep(2 * time.Second)
	exists, err := conn.Exists("strings:setget:ttl")
	assert.Nil(err)
	assert.False(exists)

	err = conn.PSetEx("strings:setget:ttl", 250*time.Millisecond, "foo")
	assert.Nil(err)
	value, err = conn.Get("strings:setget:ttl")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	time.Sleep(750 * time.Millisecond)
	exists, err = conn.Exists("strings:setget:ttl")
	assert.Nil(err)
	assert.False(exists)
}

func TestSetGetExists(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.Del("strings:setget:exists")
	err := conn.SetExExists("strings:setget:exists", time.Second, true, "foo")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
	err = conn.Set("strings:setget:exists", "bar")
	assert.Nil(err)
	err = conn.SetExExists("strings:setget:exists", time.Second, true, "foo")
	assert.Nil(err)
	value, err := conn.Get("strings:setget:exists")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	time.Sleep(2 * time.Second)
	exists, err := conn.Exists("strings:setget:exists")
	assert.Nil(err)
	assert.False(exists)
	err = conn.Set("strings:setget:exists", "bar")
	assert.Nil(err)
	err = conn.SetExExists("strings:setget:exists", time.Second, false, "foo")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))

	conn.Del("strings:setget:exists")
	err = conn.PSetExExists("strings:setget:exists", 250*time.Millisecond, true, "bar")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
	err = conn.Set("strings:setget:exists", "foo")
	assert.Nil(err)
	err = conn.PSetExExists("strings:setget:exists", 250*time.Millisecond, true, "bar")
	assert.Nil(err)
	value, err = conn.Get("strings:setget:exists")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	time.Sleep(750 * time.Millisecond)
	exists, err = conn.Exists("strings:setget:exists")
	assert.Nil(err)
	assert.False(exists)
	err = conn.Set("strings:setget:exists", "foo")
	assert.Nil(err)
	err = conn.PSetExExists("strings:setget:exists", 250*time.Millisecond, false, "bar")
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
}

func TestMSetGet(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	// Simple setting and getting.
	h := redis.NewHash().Set("strings:msetget:a", "foo").Set("strings:msetget:b", 4711)
	err := conn.MSet(h)
	assert.Nil(err)
	result, err := conn.MGet("strings:msetget:a", "strings:msetget:b", "strings:msetget:c")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "foo")
	assertEqualInt(assert, result, 1, 4711)
	assertNil(assert, result, 2)

	// Setting with existence check.
	h = redis.NewHash().Set("strings:msetget:c", true).Set("strings:msetget:d", "bar")
	err = conn.MSetNX(h)
	result, err = conn.MGet("strings:msetget:c", "strings:msetget:d")
	assert.Nil(err)
	assertEqualBool(assert, result, 0, true)
	assertEqualString(assert, result, 1, "bar")
	h = redis.NewHash().Set("strings:msetget:d", "yadda").Set("strings:msetget:e", false)
	err = conn.MSetNX(h)
	assert.True(errors.IsError(err, redis.ErrCannotSetKey))
}

func TestAppendRange(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	count, err := conn.Append("strings:append", "foo")
	assert.Nil(err)
	assert.Equal(count, 3)
	count, err = conn.Append("strings:append", "bar")
	assert.Nil(err)
	assert.Equal(count, 6)
	count, err = conn.Append("strings:append", "yadda")
	assert.Nil(err)
	assert.Equal(count, 11)

	value, err := conn.Get("strings:append")
	assert.Nil(err)
	assert.Equal(value.String(), "foobaryadda")

	length, err := conn.SetRange("strings:append", 3, " / ")
	assert.Nil(err)
	assert.Equal(length, 11)
	value, err = conn.Get("strings:append")
	assert.Nil(err)
	assert.Equal(value.String(), "foo / yadda")
	length, err = conn.StrLen("strings:append")
	assert.Nil(err)
	assert.Equal(length, 11)
	value, err = conn.GetRange("strings:append", 3, 5)
	assert.Nil(err)
	assert.Equal(value.String(), " / ")
}

func TestBit(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	count, err := conn.BitCount("strings:bit")
	assert.Nil(err)
	assert.Equal(count, 0)

	conn.Set("strings:bit", "UU")
	count, err = conn.BitCount("strings:bit")
	assert.Nil(err)
	assert.Equal(count, 8)
	count, err = conn.BitCountInterval("strings:bit", 1, 1)
	assert.Nil(err)
	assert.Equal(count, 4)

	conn.Set("strings:bit:a", "U")
	conn.Set("strings:bit:b", "<")
	conn.Set("strings:bit:c", "X")
	size, err := conn.BitOp(redis.BitOpAnd, "strings:bit:and", "strings:bit:a", "strings:bit:b")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("strings:bit:and")
	assert.Nil(err)
	assert.Equal(count, 2)
	size, err = conn.BitOp(redis.BitOpOr, "strings:bit:or", "strings:bit:a", "strings:bit:b")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("strings:bit:or")
	assert.Nil(err)
	assert.Equal(count, 6)
	size, err = conn.BitOp(redis.BitOpXOr, "strings:bit:xor", "strings:bit:a", "strings:bit:b")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("strings:bit:xor")
	assert.Nil(err)
	assert.Equal(count, 4)
	size, err = conn.BitOp(redis.BitOpNot, "strings:bit:not", "strings:bit:c")
	assert.Nil(err)
	assert.Equal(size, 1)
	count, err = conn.BitCount("strings:bit:not")
	assert.Nil(err)
	assert.Equal(count, 5)
	bit, err := conn.GetBit("strings:bit:a", 0)
	assert.Nil(err)
	assert.False(bit)
	bit, err = conn.GetBit("strings:bit:a", 1)
	assert.Nil(err)
	assert.True(bit)
}

func TestIncrDecr(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	ival, err := conn.Incr("strings:incr:a")
	assert.Nil(err)
	assert.Equal(ival, 1)
	ival, err = conn.Incr("strings:incr:a")
	assert.Nil(err)
	assert.Equal(ival, 2)
	ival, err = conn.IncrBy("strings:incr:a", 3)
	assert.Nil(err)
	assert.Equal(ival, 5)
	ival, err = conn.IncrBy("strings:incr:a", -5)
	assert.Nil(err)
	assert.Equal(ival, 0)
	ival, err = conn.Decr("strings:incr:a")
	assert.Nil(err)
	assert.Equal(ival, -1)
	ival, err = conn.DecrBy("strings:incr:a", 4)
	assert.Nil(err)
	assert.Equal(ival, -5)

	err = conn.Set("strings:incr:b", 8.15)
	assert.Nil(err)
	fval, err := conn.IncrByFloat("strings:incr:b", 47.11)
	assert.Nil(err)
	assert.Equal(fval, 55.26)

	err = conn.Set("strings:incr:c", "no number")
	assert.Nil(err)
	ival, err = conn.Incr("strings:incr:c")
	assert.True(errors.IsError(err, redis.ErrServerResponse))
}

// EOF
