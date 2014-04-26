// Tideland Go Data Management - Redis Client - Unit Tests - Key Commands
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
	"time"

	"github.com/tideland/goas/v3/errors"
	"github.com/tideland/godm/v3/redis"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestKeysDelExists(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	_, err := conn.Del("keys:del-exists")
	assert.Nil(err)
	exists, err := conn.Exists("keys:del-exists")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("keys:del-exists", "foo")
	assert.Nil(err)
	exists, err = conn.Exists("keys:del-exists")
	assert.Nil(err)
	assert.True(exists)

	count, err := conn.Del("keys:del-exists")
	assert.Nil(err)
	assert.Equal(count, 1)
	exists, err = conn.Exists("keys:del-exists")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("keys:del-exists-1", "foo")
	assert.Nil(err)
	err = conn.Set("keys:del-exists-2", "bar")
	assert.Nil(err)
	err = conn.Set("keys:del-exists-3", "yadda")
	assert.Nil(err)
	count, err = conn.Del("keys:del-exists-1", "keys:del-exists-2", "keys:del-exists-3", "keys:del-exists-4")
	assert.Nil(err)
	assert.Equal(count, 3)
}

func TestKeysExpire(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	// Standard expiring.
	err := conn.Set("keys:expire", "foo")
	assert.Nil(err)
	err = conn.Expire("keys:expire", time.Second)
	assert.Nil(err)
	exists, err := conn.Exists("keys:expire")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(1500 * time.Millisecond)

	exists, err = conn.Exists("keys:expire")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("keys:expire", "foo")
	assert.Nil(err)
	err = conn.PExpire("keys:expire", 500*time.Millisecond)
	assert.Nil(err)
	exists, err = conn.Exists("keys:expire")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(600 * time.Millisecond)

	exists, err = conn.Exists("keys:expire")
	assert.Nil(err)
	assert.False(exists)

	// Expiring at a time.
	err = conn.Set("keys:expire-at", "foo")
	assert.Nil(err)
	err = conn.ExpireAt("keys:expire-at", time.Now().Add(time.Second))
	assert.Nil(err)
	exists, err = conn.Exists("keys:expire-at")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(1500 * time.Millisecond)

	exists, err = conn.Exists("keys:expire-at")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("keys:expire-at", "foo")
	assert.Nil(err)
	err = conn.PExpireAt("keys:expire-at", time.Now().Add(500*time.Millisecond))
	assert.Nil(err)
	exists, err = conn.Exists("keys:expire-at")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(600 * time.Millisecond)

	exists, err = conn.Exists("keys:expire-at")
	assert.Nil(err)
	assert.False(exists)

	// Persisting and TTL.
	err = conn.Set("keys:expire", "foo")
	assert.Nil(err)
	err = conn.Expire("keys:expire", time.Second)
	assert.Nil(err)
	exists, err = conn.Exists("keys:expire")
	assert.Nil(err)
	assert.True(exists)
	ttl, err := conn.TTL("keys:expire")
	assert.Nil(err)
	assert.True(ttl > 0)
	ttl, err = conn.PTTL("keys:expire")
	assert.Nil(err)
	assert.True(ttl > 0)
	err = conn.Persist("keys:expire")
	assert.Nil(err)

	time.Sleep(2 * time.Second)

	exists, err = conn.Exists("keys:expire")
	assert.Nil(err)
	assert.True(exists)
}

func TestDumpRestore(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	err := conn.Set("keys:dump", "foo")
	assert.Nil(err)
	raw, err := conn.Dump("keys:dump")
	assert.Nil(err)
	_, err = conn.Del("keys:dump")
	assert.Nil(err)
	exists, err := conn.Exists("keys:dump")
	assert.Nil(err)
	assert.False(exists)
	err = conn.Restore("keys:dump", 0, raw)
	assert.Nil(err)
	value, err := conn.Get("keys:dump")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestRename(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	err := conn.Set("keys:rename:old", "foo")
	assert.Nil(err)
	err = conn.Rename("keys:rename:old", "keys:rename:new")
	assert.Nil(err)
	value, err := conn.Get("keys:rename:new")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	err = conn.Set("keys:rename:old", "bar")
	assert.Nil(err)
	err = conn.RenameNX("keys:rename:new", "keys:rename:old")
	assert.True(errors.IsError(err, redis.ErrCannotRenameKey))
	value, err = conn.Get("keys:rename:old")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
}

func TestKeys(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.Set("keys:random:a", "foo")
	conn.Set("keys:random:b", "bar")
	conn.Set("keys:random:c", "yadda")

	keys, err := conn.Keys("*")
	assert.Nil(err)
	assert.Length(keys, 3)
	keys, err = conn.Keys("*a*")
	assert.Nil(err)
	assert.Length(keys, 3)
	keys, err = conn.Keys("*a")
	assert.Nil(err)
	assert.Length(keys, 1)

	key, err := conn.RandomKey()
	assert.Nil(err)
	assert.Substring("random", key)
}

func TestScan(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateKeyData(assert, conn, "abcdefghij")

	assertScan := func(pattern string, count, total int) {
		var cursor int
		var keys []string
		var err error
		var max, all int

		if count == 0 {
			max = 20
		} else {
			max = count * 2
		}

		for {
			cursor, keys, err = conn.Scan(cursor, pattern, count)
			assert.Nil(err)
			all += len(keys)
			assert.True(len(keys) <= max)
			if cursor == 0 {
				break
			}
		}
		assert.Equal(all, total)
	}

	assertScan("", 0, 100)
	assertScan("", 20, 100)
	assertScan("key-scan:a*", 0, 10)
	assertScan("key-scan:a*", 5, 10)
	assertScan("key-scan:-*-", 0, 0)
	assertScan("key-scan:-*-", 20, 0)
}

func TestType(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	// String type.
	err := conn.Set("keys:type:a", "foo")
	assert.Nil(err)
	keytype, err := conn.Type("keys:type:a")
	assert.Nil(err)
	assert.Equal(keytype, "string")
}

//--------------------
// TOOLS
//--------------------

// generateKeyData generates a number of sets and values.
func generateKeyData(assert asserts.Assertion, conn *redis.Connection, charset string) {
	for i := 0; i < 10; i++ {
		for _, c := range charset {
			key := fmt.Sprintf("key-scan:%c%d", c, i)
			value := fmt.Sprintf("test data %c%d", c, i)
			err := conn.Set(key, value)
			assert.Nil(err)
		}
	}
}

// EOF
