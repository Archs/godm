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

	count, err := conn.LPush("lists:left", "foo", "bar")
	assert.Nil(err)
	assert.Equal(count, 2)
	count, err = conn.LPush("lists:left", 42, 4711)
	assert.Nil(err)
	assert.Equal(count, 4)

	value, err := conn.LPop("lists:left")
	assert.Nil(err)
	num, err := value.Int()
	assert.Nil(err)
	assert.Equal(num, 4711)
	value, err = conn.LPop("lists:left")
	assert.Nil(err)
	num, err = value.Int()
	assert.Nil(err)
	assert.Equal(num, 42)
	value, err = conn.LPop("lists:left")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	value, err = conn.LPop("lists:left")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestBLPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	go func() {
		conn, restore := connectDatabase(assert)
		defer restore()

		time.Sleep(500 * time.Millisecond)
		length, err := conn.LPush("lists:blpop", "foo", "bar", "yadda")
		assert.Nil(err)
		assert.Equal(length, 3)
	}()

	conn, restore := connectDatabase(assert)
	defer restore()

	key, value, err := conn.BLPop(time.Second, "lists:blpop", "lists:brpop")
	assert.Nil(err)
	assert.Equal(key, "lists:blpop")
	assert.Equal(value.String(), "yadda")

	for i := 0; i < 2; i++ {
		_, _, err = conn.BLPop(time.Second, "lists:blpop", "lists:brpop")
		assert.Nil(err)
	}

	key, value, err = conn.BLPop(time.Second, "lists:blpop", "lists:brpop")
	assert.True(errors.IsError(err, redis.ErrTimeout))
	assert.Equal(key, "")
	assert.Nil(value)
}

func TestRPushRPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	count, err := conn.RPush("lists:right", "foo", "bar")
	assert.Nil(err)
	assert.Equal(count, 2)
	count, err = conn.RPush("lists:right", 42, 4711)
	assert.Nil(err)
	assert.Equal(count, 4)

	value, err := conn.RPop("lists:right")
	assert.Nil(err)
	num, err := value.Int()
	assert.Nil(err)
	assert.Equal(num, 4711)
	value, err = conn.RPop("lists:right")
	assert.Nil(err)
	num, err = value.Int()
	assert.Nil(err)
	assert.Equal(num, 42)
	value, err = conn.RPop("lists:right")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	value, err = conn.RPop("lists:right")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestBRPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	go func() {
		conn, restore := connectDatabase(assert)
		defer restore()

		time.Sleep(500 * time.Millisecond)
		length, err := conn.LPush("lists:brpop", "foo", "bar", "yadda")
		assert.Nil(err)
		assert.Equal(length, 3)
	}()

	conn, restore := connectDatabase(assert)
	defer restore()

	key, value, err := conn.BRPop(time.Second, "lists:blpop", "lists:brpop")
	assert.Nil(err)
	assert.Equal(key, "lists:brpop")
	assert.Equal(value.String(), "foo")

	for i := 0; i < 2; i++ {
		_, _, err = conn.BLPop(time.Second, "lists:blpop", "lists:brpop")
		assert.Nil(err)
	}

	key, value, err = conn.BRPop(time.Second, "lists:blpop", "lists:brpop")
	assert.True(errors.IsError(err, redis.ErrTimeout))
	assert.Equal(key, "")
	assert.Nil(value)
}

func TestLPushXRPushX(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("lists:exists", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)
	length, err = conn.LPushX("lists:exists", "left-baz")
	assert.Nil(err)
	assert.Equal(length, 4)
	length, err = conn.LPushX("lists:exists", "right-baz")
	assert.Nil(err)
	assert.Equal(length, 5)

	length, err = conn.LPushX("lists:exists-not", "no-baz")
	assert.Nil(err)
	assert.Equal(length, 0)
	length, err = conn.RPushX("lists:exists-not", "no-baz")
	assert.Nil(err)
	assert.Equal(length, 0)
}

func TestLIndex(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("lists:index", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)

	value, err := conn.LIndex("lists:index", 0)
	assert.Nil(err)
	assert.Equal(value.String(), "yadda")
	value, err = conn.LIndex("lists:index", 2)
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestLInsertRangeRemTrim(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("lists:insert", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)

	length, err = conn.LInsertBefore("lists:insert", "bar", "baz")
	assert.Nil(err)
	assert.Equal(length, 4)
	length, err = conn.LInsertAfter("lists:insert", "bar", "buf")
	assert.Nil(err)
	assert.Equal(length, 5)

	length, err = conn.LLen("lists:insert")
	assert.Nil(err)
	assert.Equal(length, 5)

	result, err := conn.LRange("lists:insert", 1, 3)
	assert.Nil(err)
	assert.Length(result, 3)
	assertEqualString(assert, result, 0, "baz")
	assertEqualString(assert, result, 1, "bar")
	assertEqualString(assert, result, 2, "buf")

	length, err = conn.LInsertBefore("lists:insert", "foo", "yadda")
	assert.Nil(err)
	assert.Equal(length, 6)
	length, err = conn.LRem("lists:insert", 0, "yadda")
	assert.Nil(err)
	assert.Equal(length, 2)

	err = conn.LTrim("lists:insert", 1, 2)
	assert.Nil(err)
	length, err = conn.LLen("lists:insert")
	assert.Nil(err)
	assert.Equal(length, 2)
	result, err = conn.LRange("lists:insert", 0, 1)
	assert.Nil(err)
	assert.Length(result, 2)
	assertEqualString(assert, result, 0, "bar")
	assertEqualString(assert, result, 1, "buf")
}

func TestRPopLPush(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	length, err := conn.LPush("lists:pop", "foo", "bar", "yadda")
	assert.Nil(err)
	assert.Equal(length, 3)

	poppush := func(expected string, lenPop, lenPush int) {
		value, err := conn.RPopLPush("lists:pop", "lists:push")
		assert.Nil(err)
		assert.Equal(value.String(), expected)
		length, err = conn.LLen("lists:pop")
		assert.Nil(err)
		assert.Equal(length, lenPop)
		length, err = conn.LLen("lists:push")
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
		length, err := conn.LPush("lists:bpop", "foo", "bar", "yadda")
		assert.Nil(err)
		assert.Equal(length, 3)
	}()

	conn, restore := connectDatabase(assert)
	defer restore()

	value, err := conn.BRPopLPush("lists:bpop", "lists:bpush", time.Second)
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	value, err = conn.BRPopLPush("lists:bpop", "lists:bpush", time.Second)
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
	value, err = conn.BRPopLPush("lists:bpop", "lists:bpush", time.Second)
	assert.Nil(err)
	assert.Equal(value.String(), "yadda")

	length, err := conn.LLen("lists:bpush")
	assert.Nil(err)
	assert.Equal(length, 3)

	value, err = conn.BRPopLPush("lists:bpop", "lists:bpush", time.Second)
	assert.True(errors.IsError(err, redis.ErrTimeout))
	assert.Nil(value)
}

// EOF
