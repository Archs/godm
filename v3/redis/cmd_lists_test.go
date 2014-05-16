// Tideland Go Data Management - Redis Client - Unit Tests - List Commands
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

func TestLPushLPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	count, err := conn.LPush("l:left", "foo", "bar")
	assert.Nil(err)
	assert.Equal(count, 2)
	count, err = conn.LPush("l:left", 42, 4711)
	assert.Nil(err)
	assert.Equal(count, 4)

	value, err := conn.LPop("l:left")
	assert.Nil(err)
	num, err := value.Int()
	assert.Nil(err)
	assert.Equal(num, 4711)
	value, err = conn.LPop("l:left")
	assert.Nil(err)
	num, err = value.Int()
	assert.Nil(err)
	assert.Equal(num, 42)
	value, err = conn.LPop("l:left")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	value, err = conn.LPop("l:left")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestBLPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	go func() {
		conn, restore := connectDatabase(assert)
		defer restore()

		time.Sleep(500 * time.Millisecond)
		length, err := conn.LPush("l:blpop", "foo", "bar", "yadda")
		assert.Nil(err)
		assert.Equal(length, 3)
	}()

	conn, restore := connectDatabase(assert)
	defer restore()

	key, value, err := conn.BLPop(time.Second, "l:blpop", "l:brpop")
	assert.Nil(err)
	assert.Equal(key, "l:blpop")
	assert.Equal(value.String(), "yadda")

	for i := 0; i < 2; i++ {
		_, _, err = conn.BLPop(time.Second, "l:blpop", "l:brpop")
		assert.Nil(err)
	}

	key, value, err = conn.BLPop(time.Second, "l:blpop", "l:brpop")
	assert.True(errors.IsError(err, redis.ErrTimeout))
	assert.Equal(key, "")
	assert.Nil(value)
}

func TestRPushRPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	count, err := conn.RPush("l:right", "foo", "bar")
	assert.Nil(err)
	assert.Equal(count, 2)
	count, err = conn.RPush("l:right", 42, 4711)
	assert.Nil(err)
	assert.Equal(count, 4)

	value, err := conn.RPop("l:right")
	assert.Nil(err)
	num, err := value.Int()
	assert.Nil(err)
	assert.Equal(num, 4711)
	value, err = conn.RPop("l:right")
	assert.Nil(err)
	num, err = value.Int()
	assert.Nil(err)
	assert.Equal(num, 42)
	value, err = conn.RPop("l:right")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	value, err = conn.RPop("l:right")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestBRPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	go func() {
		conn, restore := connectDatabase(assert)
		defer restore()

		time.Sleep(500 * time.Millisecond)
		length, err := conn.LPush("l:brpop", "foo", "bar", "yadda")
		assert.Nil(err)
		assert.Equal(length, 3)
	}()

	conn, restore := connectDatabase(assert)
	defer restore()

	key, value, err := conn.BRPop(time.Second, "l:blpop", "l:brpop")
	assert.Nil(err)
	assert.Equal(key, "l:brpop")
	assert.Equal(value.String(), "foo")

	for i := 0; i < 2; i++ {
		_, _, err = conn.BLPop(time.Second, "l:blpop", "l:brpop")
		assert.Nil(err)
	}

	key, value, err = conn.BRPop(time.Second, "l:blpop", "l:brpop")
	assert.True(errors.IsError(err, redis.ErrTimeout))
	assert.Equal(key, "")
	assert.Nil(value)
}

func TestLPushXRPushX(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("l:exists", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)
	length, err = conn.LPushX("l:exists", "left-baz")
	assert.Nil(err)
	assert.Equal(length, 4)
	length, err = conn.LPushX("l:exists", "right-baz")
	assert.Nil(err)
	assert.Equal(length, 5)

	length, err = conn.LPushX("l:exists-not", "no-baz")
	assert.Nil(err)
	assert.Equal(length, 0)
	length, err = conn.RPushX("l:exists-not", "no-baz")
	assert.Nil(err)
	assert.Equal(length, 0)
}

func TestLIndex(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("l:index", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)

	value, err := conn.LIndex("l:index", 0)
	assert.Nil(err)
	assert.Equal(value.String(), "yadda")
	value, err = conn.LIndex("l:index", 2)
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestLInsertRangeRemTrim(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("l:insert", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)

	length, err = conn.LInsertBefore("l:insert", "bar", "baz")
	assert.Nil(err)
	assert.Equal(length, 4)
	length, err = conn.LInsertAfter("l:insert", "bar", "buf")
	assert.Nil(err)
	assert.Equal(length, 5)

	length, err = conn.LLen("l:insert")
	assert.Nil(err)
	assert.Equal(length, 5)

	result, err := conn.LRange("l:insert", 1, 3)
	assert.Nil(err)
	assert.Length(result, 3)
	assertEqualString(assert, result, 0, "baz")
	assertEqualString(assert, result, 1, "bar")
	assertEqualString(assert, result, 2, "buf")

	length, err = conn.LInsertBefore("l:insert", "foo", "yadda")
	assert.Nil(err)
	assert.Equal(length, 6)
	length, err = conn.LRem("l:insert", 0, "yadda")
	assert.Nil(err)
	assert.Equal(length, 2)

	err = conn.LTrim("l:insert", 1, 2)
	assert.Nil(err)
	length, err = conn.LLen("l:insert")
	assert.Nil(err)
	assert.Equal(length, 2)
	result, err = conn.LRange("l:insert", 0, 1)
	assert.Nil(err)
	assert.Length(result, 2)
	assertEqualString(assert, result, 0, "bar")
	assertEqualString(assert, result, 1, "buf")
}

func TestRPopLPush(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("l:pop", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)

	poppush := func(expected string, lenPop, lenPush int) {
		value, err := conn.RPopLPush("l:pop", "l:push")
		assert.Nil(err)
		assert.Equal(value.String(), expected)
		length, err = conn.LLen("l:pop")
		assert.Nil(err)
		assert.Equal(length, lenPop)
		length, err = conn.LLen("l:push")
		assert.Nil(err)
		assert.Equal(length, lenPush)
	}
	poppush("foo", 2, 1)
	poppush("bar", 1, 2)
	poppush("yadda", 0, 3)
	poppush("(nil)", 0, 3)
}

func TestBRPopLPush(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	go func() {
		conn, restore := connectDatabase(assert)
		defer restore()

		time.Sleep(500 * time.Millisecond)
		length, err := conn.LPush("l:bpop", "foo", "bar", "yadda")
		assert.Nil(err)
		assert.Equal(length, 3)
	}()

	conn, restore := connectDatabase(assert)
	defer restore()

	value, err := conn.BRPopLPush("l:bpop", "l:bpush", time.Second)
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	value, err = conn.BRPopLPush("l:bpop", "l:bpush", time.Second)
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	value, err = conn.BRPopLPush("l:bpop", "l:bpush", time.Second)
	assert.Nil(err)
	assert.Equal(value.String(), "yadda")

	length, err := conn.LLen("l:bpush")
	assert.Nil(err)
	assert.Equal(length, 3)

	value, err = conn.BRPopLPush("l:bpop", "l:bpush", time.Second)
	assert.True(errors.IsError(err, redis.ErrTimeout))
	assert.Nil(value)
}

// EOF
