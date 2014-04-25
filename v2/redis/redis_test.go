// Tideland Go Data Management - Redis Client - Unit Tests
//
// Copyright (C) 2009-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis_test

//--------------------
// IMPORTS
//--------------------

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tideland/goas/v2/logger"
	"github.com/tideland/godm/v2/redis"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestConnection(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	rs, err := db.Command("echo", "Hello, World!")
	assert.Nil(err)
	assert.Equal(rs.FirstValue().String(), "Hello, World!")
	rs, err = db.Command("ping")
	assert.Nil(err)
	assert.Equal(rs.FirstValue().String(), "PONG")
}

func TestUnixSocketConnection(t *testing.T) {
	if _, err := os.Stat("/tmp/redis.sock"); os.IsNotExist(err) {
		return
	}

	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(&redis.Configuration{
		Address:     "/tmp/redis.sock",
		UnixSockets: true,
	})
	assert.Nil(err)

	rs, err := db.Command("echo", "Hello, World!")
	assert.Nil(err)
	assert.Equal(rs.FirstValue().String(), "Hello, World!")
	rs, err = db.Command("ping")
	assert.Nil(err)
	assert.Equal(rs.FirstValue().String(), "PONG")
}

func TestSimpleSingleValue(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	rs, err := db.Command("del", "single-value")
	assert.Nil(err)
	_, err = db.Command("get", "single-value")
	assert.True(redis.IsKeyNotFoundError(err))
	rs, err = db.Command("set", "single-value", "Hello, World!")
	assert.Nil(err)
	rs, err = db.Command("get", "single-value")
	assert.Nil(err)
	assert.Length(rs, 1)
	assert.Equal(rs.FirstValue().String(), "Hello, World!")

	db.Command("del", "single-exists")
	rs, err = db.Command("setnx", "single-exists", "foo")
	assert.Nil(err)
	isSet, err := rs.FirstValue().Bool()
	assert.Nil(err)
	assert.True(isSet, "'setnx' returned true")
	rs, err = db.Command("setnx", "single-exists", "bar")
	assert.Nil(err)
	isSet, err = rs.FirstValue().Bool()
	assert.Nil(err)
	assert.False(isSet, "'setnx' returned false")
}

func TestSimpleMultipleValues(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	// Simple read of multiple keys.
	db.Command("del", "multiple-value:1")
	db.Command("del", "multiple-value:2")
	db.Command("del", "multiple-value:3")
	db.Command("del", "multiple-value:4")
	db.Command("del", "multiple-value:5")

	db.Command("set", "multiple-value:1", "one")
	db.Command("set", "multiple-value:2", "two")
	db.Command("set", "multiple-value:3", "three")
	db.Command("set", "multiple-value:4", "four")
	db.Command("set", "multiple-value:5", "five")

	rs, err := db.Command("mget", "multiple-value:1", "multiple-value:2", "multiple-value:3", "multiple-value:4", "multiple-value:5")
	assert.Nil(err)
	assert.Length(rs, 5)
	assert.Equal(rs[0].String(), "one")
	assert.Equal(rs[1].String(), "two")
	assert.Equal(rs[2].String(), "three")
	assert.Equal(rs[3].String(), "four")
	assert.Equal(rs[4].String(), "five")

	// Read sorted set with keys and values (scores).
	db.Command("del", "sorted-set")

	db.Command("zadd", "sorted-set", 16, "one")
	db.Command("zadd", "sorted-set", 8, "two")
	db.Command("zadd", "sorted-set", 4, "three")
	db.Command("zadd", "sorted-set", 2, "four")
	db.Command("zadd", "sorted-set", 1, "five")

	rs, err = db.Command("zrevrange", "sorted-set", 0, 10, "withscores")
	assert.Nil(err)
	assert.Length(rs, 10)
	kvs := rs.KeyValues()
	assert.Equal(kvs[0].Key, "one")
	assert.Equal(kvs[0].Value.String(), "16")
	assert.Equal(kvs[4].Key, "five")
	assert.Equal(kvs[4].Value.String(), "1")
}

func TestHash(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	db.Command("del", "hash:manual")
	db.Command("del", "hash:hashable")

	// Manual hash usage.
	db.Command("hset", "hash:manual", "field:1", "one")
	db.Command("hset", "hash:manual", "field:2", "two")

	rs, err := db.Command("hget", "hash:manual", "field:1")
	assert.Nil(err)
	assert.Length(rs, 1)
	assert.Equal(rs.FirstValue().String(), "one")

	rs, err = db.Command("hgetall", "hash:manual")
	assert.Nil(err)
	assert.Length(rs, 4)
	assert.Equal(rs[0].String(), "field:1")
	assert.Equal(rs[1].String(), "one")
	assert.Equal(rs[2].String(), "field:2")
	assert.Equal(rs[3].String(), "two")

	// Use the Hash type and the Hashable interface.
	rs, err = db.Command("hgetall", "hash:manual")
	assert.Nil(err)
	h := rs.Hash()
	assert.Equal(h.Len(), 2)
	v, err := h.String("field:1")
	assert.Nil(err)
	assert.Equal(v, "one")
	v, err = h.String("field:2")
	assert.Nil(err)
	assert.Equal(v, "two")
	v, err = h.String("field:not-existing")
	assert.True(redis.IsInvalidKeyError(err))

	htIn := hashableTestType{"foo \"bar\" yadda", 4711, true, 8.15}
	db.Command("hmset", "hash:hashable", htIn.GetHash())
	db.Command("hincrby", "hash:hashable", "hashable:field:b", 289)

	htOut := hashableTestType{}
	rs, err = db.Command("hgetall", "hash:hashable")
	assert.Nil(err)
	htOut.SetHash(rs.Hash())
	assert.Equal(htOut.a, "foo \"bar\" yadda")
	assert.Equal(htOut.b, int64(5000))
	assert.True(htOut.c)
	assert.Equal(htOut.d, 8.15)
}

func TestFuture(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	db.Command("del", "future")

	fut := db.AsyncCommand("rpush", "future", "one", "two", "three", "four", "five")
	rs, err := fut.ResultSet()
	assert.Nil(err)
	v, err := rs.FirstValue().Int()
	assert.Nil(err)
	assert.Equal(v, 5)
}

func TestStringSlice(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	db.Command("del", "string:slice")

	sliceIn := []string{"A", "B", "C", "D", "E"}
	rs, err := db.Command("set", "string:slice", sliceIn)
	assert.Nil(err)

	rs, err = db.Command("get", "string:slice")
	assert.Nil(err)
	sliceOut := rs.FirstValue().StringSlice()
	assert.Length(sliceOut, 5)
	assert.Equal(sliceOut, sliceIn)
}

func TestStringMap(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	db.Command("del", "string:map")

	mapIn := map[string]string{
		"A": "1",
		"B": "2",
		"C": "3",
		"D": "4",
		"E": "5",
	}
	rs, err := db.Command("set", "string:map", mapIn)
	assert.Nil(err)

	rs, err = db.Command("get", "string:map")
	assert.Nil(err)
	mapOut := rs.FirstValue().StringMap()
	assert.Length(mapOut, 5)
	assert.Equal(mapOut, mapIn)
}

func TestMultiCommand(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	db.Command("del", "multi-command:1")
	db.Command("del", "multi-command:2")
	db.Command("del", "multi-command:3")
	db.Command("del", "multi-command:4")
	db.Command("del", "multi-command:5")

	rss, err := db.MultiCommand(func(mc redis.MultiCommand) error {
		mc.Command("set", "multi-command:1", "1")
		mc.Command("set", "multi-command:1", "2")
		mc.Discard()
		mc.Command("set", "multi-command:1", "one")
		mc.Command("set", "multi-command:2", "two")
		mc.Command("set", "multi-command:3", "three")
		mc.Command("set", "multi-command:4", "four")
		mc.Command("set", "multi-command:5", "five")

		mc.Command("get", "multi-command:3")
		mc.Command("mget", "multi-command:1", "multi-command:2", "multi-command:5")

		return nil
	})
	assert.Nil(err)
	assert.Length(rss, 7)
	assert.Equal(rss[5].FirstValue().String(), "three")
	assert.Length(rss[6], 3)
}

func TestBlockingPop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	terms := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrott", "golf", "hotel", "india", "juliett",
	}

	db.Command("del", "queue")

	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			_, err := db.Command("lpush", "queue", terms[i])
			assert.Nil(err)
		}
	}()

	for i := 0; i < 10; i++ {
		rs, err := db.Command("brpop", "queue", 5)
		assert.Nil(err)
		assert.Equal(rs[0].String(), "queue", "Right 'queue' has been returned.")
		term := rs[1].String()
		assert.Equal(term, terms[i], "Popped value is ok.")
	}

	_, err = db.Command("brpop", "queue", 1)
	assert.True(redis.IsTimeoutError(err), "'brpop' timed out.")
}

func TestPubSub(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(&redis.Configuration{LogCommands: true})
	assert.Nil(err)

	sub, err := db.Subscribe("pubsub:1", "pubsub:2", "pubsub:3")
	assert.Nil(err, "No error when subscribing.")
	sub.Subscribe("pubsub:pattern:*")

	go func() {
		time.Sleep(500 * time.Millisecond)
		db.Publish("pubsub:1", "foo")
		db.Publish("pubsub:2", "bar")
		db.Publish("pubsub:3", "baz")
		db.Publish("pubsub:pattern:yadda", "yadda")
	}()

	// Check published value receiving.
	publishing := <-sub.Publishings()
	assert.Equal(publishing.Channel(), "pubsub:1", "First value channel has been ok.")
	assert.Equal(publishing.Value().String(), "foo", "First value has been ok.")

	publishing = <-sub.Publishings()
	assert.Equal(publishing.Channel(), "pubsub:2", "Second value channel has been ok.")
	assert.Equal(publishing.Value().String(), "bar", "Second value has been ok.")

	publishing = <-sub.Publishings()
	assert.Equal(publishing.Channel(), "pubsub:3", "Third value channel has been ok.")
	assert.Equal(publishing.Value().String(), "baz", "Third value has been ok.")

	publishing = <-sub.Publishings()
	assert.Equal(publishing.Channel(), "pubsub:pattern:yadda", "Fourth value channel has been ok.")
	assert.Equal(publishing.ChannelPattern(), "pubsub:pattern:*", "Fourth value channel pattern has been ok.")
	assert.Equal(publishing.Value().String(), "yadda", "Fourth value has been ok.")

	// Check no more receiving.
	select {
	case publishing = <-sub.Publishings():
		assert.Nil(publishing, "Nothing expected here.")
	case <-time.After(200 * time.Millisecond):
		assert.True(true, "Timeout like expected.")
	}

	// Check unsubscribing.
	sub.Unsubscribe("pubsub:3")

	go func() {
		time.Sleep(50 * time.Millisecond)
		db.Publish("pubsub:3", "foobar")
	}()

	select {
	case publishing = <-sub.Publishings():
		assert.Nil(publishing, "Nothing expected here.")
	case <-time.After(200 * time.Millisecond):
		assert.True(true, "Timeout like expected.")
	}

	// Check subscription closing.
	sub.Close()

	select {
	case _, ok := <-sub.Publishings():
		assert.False(ok, "Expected signalling of closed channel.")
	case <-time.After(200 * time.Millisecond):
		assert.False(true, "Timeout not expected here.")
	}
}

// Test illegal databases.
func TestIllegalDatabases(t *testing.T) {
	if testing.Short() {
		return
	}

	// Test illegal database number.
	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(&redis.Configuration{Database: 999999})
	assert.Nil(err)

	// Test illegal network address.
	db, err = redis.Connect(&redis.Configuration{Address: "192.168.100.100:12345"})

	_, err = db.Command("ping")
	assert.ErrorMatch(err, `\[E.*\] cannot establish connection: dial tcp 192.168.100.100:12345: i/o timeout`)
	assert.True(redis.IsConnectionError(err))
}

// Test a long run to check stopping and restarting redis.
func TestLongRun(t *testing.T) {
	if testing.Short() {
		return
	}

	assert := asserts.NewTestingAssertion(t, true)
	db, err := redis.Connect(nil)
	assert.Nil(err)

	wait := make(chan bool)

	for i := 0; i < 100; i++ {
		go func(ii int) {
			for j := 0; j < 20; j++ {
				key := fmt.Sprintf("long-run:%d:%d", ii, j)
				logger.Debugf("key: %s", key)
				_, err := db.Command("set", key, ii+j)
				if err != nil {
					logger.Errorf("%v", err)
				}
				time.Sleep(time.Second)
				if ii == 99 && j == 19 {
					wait <- true
				}
			}
		}(i)
	}

	<-wait
	rs, err := db.Command("exists", "long-run:99:19")
	assert.Nil(err)
	exists, err := rs.FirstValue().Bool()
	assert.Nil(err)
	assert.True(exists)
}

//--------------------
// HELPER
//--------------------

// hashableTestType is a simple type implementing the
// Hashable interface.
type hashableTestType struct {
	a string
	b int64
	c bool
	d float64
}

// GetHash returns the fields as hash.
func (htt *hashableTestType) GetHash() redis.Hash {
	h := redis.NewHash()

	h.Set("hashable:field:a", htt.a)
	h.Set("hashable:field:b", htt.b)
	h.Set("hashable:field:c", htt.c)
	h.Set("hashable:field:d", htt.d)

	return h
}

// SetHash sets the fields from a hash.
func (htt *hashableTestType) SetHash(h redis.Hash) {
	htt.a, _ = h.String("hashable:field:a")
	htt.b, _ = h.Int64("hashable:field:b")
	htt.c, _ = h.Bool("hashable:field:c")
	htt.d, _ = h.Float64("hashable:field:d")
}

// EOF
