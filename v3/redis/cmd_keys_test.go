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

	_, err := conn.Del("k:del-exists")
	assert.Nil(err)
	exists, err := conn.Exists("k:del-exists")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("k:del-exists", "foo")
	assert.Nil(err)
	exists, err = conn.Exists("k:del-exists")
	assert.Nil(err)
	assert.True(exists)

	count, err := conn.Del("k:del-exists")
	assert.Nil(err)
	assert.Equal(count, 1)
	exists, err = conn.Exists("k:del-exists")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("k:del-exists-1", "foo")
	assert.Nil(err)
	err = conn.Set("k:del-exists-2", "bar")
	assert.Nil(err)
	err = conn.Set("k:del-exists-3", "yadda")
	assert.Nil(err)
	count, err = conn.Del("k:del-exists-1", "k:del-exists-2", "k:del-exists-3", "k:del-exists-4")
	assert.Nil(err)
	assert.Equal(count, 3)
}

func TestKeysExpire(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	// Standard expiring.
	err := conn.Set("k:expire", "foo")
	assert.Nil(err)
	err = conn.Expire("k:expire", time.Second)
	assert.Nil(err)
	exists, err := conn.Exists("k:expire")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(1500 * time.Millisecond)

	exists, err = conn.Exists("k:expire")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("k:expire", "foo")
	assert.Nil(err)
	err = conn.PExpire("k:expire", 500*time.Millisecond)
	assert.Nil(err)
	exists, err = conn.Exists("k:expire")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(600 * time.Millisecond)

	exists, err = conn.Exists("k:expire")
	assert.Nil(err)
	assert.False(exists)

	// Expiring at a time.
	err = conn.Set("k:expire-at", "foo")
	assert.Nil(err)
	err = conn.ExpireAt("k:expire-at", time.Now().Add(time.Second))
	assert.Nil(err)
	exists, err = conn.Exists("k:expire-at")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(1500 * time.Millisecond)

	exists, err = conn.Exists("k:expire-at")
	assert.Nil(err)
	assert.False(exists)

	err = conn.Set("k:expire-at", "foo")
	assert.Nil(err)
	err = conn.PExpireAt("k:expire-at", time.Now().Add(500*time.Millisecond))
	assert.Nil(err)
	exists, err = conn.Exists("k:expire-at")
	assert.Nil(err)
	assert.True(exists)

	time.Sleep(600 * time.Millisecond)

	exists, err = conn.Exists("k:expire-at")
	assert.Nil(err)
	assert.False(exists)

	// Persisting and TTL.
	err = conn.Set("k:expire", "foo")
	assert.Nil(err)
	err = conn.Expire("k:expire", time.Second)
	assert.Nil(err)
	exists, err = conn.Exists("k:expire")
	assert.Nil(err)
	assert.True(exists)
	ttl, err := conn.TTL("k:expire")
	assert.Nil(err)
	assert.True(ttl > 0)
	ttl, err = conn.PTTL("k:expire")
	assert.Nil(err)
	assert.True(ttl > 0)
	err = conn.Persist("k:expire")
	assert.Nil(err)

	time.Sleep(2 * time.Second)

	exists, err = conn.Exists("k:expire")
	assert.Nil(err)
	assert.True(exists)
}

func TestDumpRestore(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	err := conn.Set("k:dump", "foo")
	assert.Nil(err)
	raw, err := conn.Dump("k:dump")
	assert.Nil(err)
	_, err = conn.Del("k:dump")
	assert.Nil(err)
	exists, err := conn.Exists("k:dump")
	assert.Nil(err)
	assert.False(exists)
	err = conn.Restore("k:dump", 0, raw)
	assert.Nil(err)
	value, err := conn.Get("k:dump")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
}

func TestRename(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	err := conn.Set("k:rename:old", "foo")
	assert.Nil(err)
	err = conn.Rename("k:rename:old", "k:rename:new")
	assert.Nil(err)
	value, err := conn.Get("k:rename:new")
	assert.Nil(err)
	assert.Equal(value.String(), "foo")
	err = conn.Set("k:rename:old", "bar")
	assert.Nil(err)
	err = conn.RenameNX("k:rename:new", "k:rename:old")
	assert.True(errors.IsError(err, redis.ErrCannotRenameKey))
	value, err = conn.Get("k:rename:old")
	assert.Nil(err)
	assert.Equal(value.String(), "bar")
}

func TestKeys(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.Set("k:random:a", "foo")
	conn.Set("k:random:b", "bar")
	conn.Set("k:random:c", "yadda")

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
	err := conn.Set("k:type:a", "foo")
	assert.Nil(err)
	keytype, err := conn.Type("k:type:a")
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
