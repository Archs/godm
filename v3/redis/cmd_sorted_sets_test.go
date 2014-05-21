// Tideland Go Data Management - Redis Client - Unit Tests - Sorted Set Commands
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

	"github.com/tideland/godm/v3/redis"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestZAddRemCard(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.ZAdd("z:add-rem-card", 1, "a", 2, "b", 3, "c", 4, "d")
	conn.ZAdd("z:add-rem-card", 5, "e", 6, "f", 7, "g", 8, "h")

	card, err := conn.ZCard("z:add-rem-card")
	assert.Nil(err)
	assert.Equal(card, 8)
	card, err = conn.ZCard("z:add-rem-card:not-existing")
	assert.Nil(err)
	assert.Equal(card, 0)

	count, err := conn.ZCount("z:add-rem-card", 3, 7, redis.InclusiveMax)
	assert.Nil(err)
	assert.Equal(count, 4)

	removed, err := conn.ZRem("z:add-rem-card", "d", "e", "i")
	assert.Nil(err)
	assert.Equal(removed, 2)
	count, err = conn.ZCount("z:add-rem-card", 3, 7, redis.InclusiveMax)
	assert.Nil(err)
	assert.Equal(count, 2)
}

func TestZIncrBy(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	added, err := conn.ZAdd("z:incr-by", 1, "foo")
	assert.Nil(err)
	assert.Equal(added, 1)
	value, err := conn.ZIncrBy("z:incr-by", 1.5, "foo")
	assert.Nil(err)
	assert.Equal(value, "2.5")
}

func TestZInterStore(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateSortedSetData(assert, conn, "z:inter:a", "abcde")
	generateSortedSetData(assert, conn, "z:inter:b", "c")
	generateSortedSetData(assert, conn, "z:inter:c", "acf")

	tests := []struct {
		key       string
		weights   []float64
		aggregate redis.Aggregate
		card      int
		expected  func(int) float64
	}{
		{
			key:       "z:inter:r1",
			weights:   nil,
			aggregate: redis.AggregateSum,
			card:      10,
			expected: func(i int) float64 {
				return float64((i + 1) * 3)
			},
		}, {
			key:       "z:inter:r2",
			weights:   nil,
			aggregate: redis.AggregateMin,
			card:      10,
			expected: func(i int) float64 {
				return float64(i + 1)
			},
		}, {
			key:       "z:inter:r3",
			weights:   nil,
			aggregate: redis.AggregateMax,
			card:      10,
			expected: func(i int) float64 {
				return float64(i + 1)
			},
		},
	}

	for _, test := range tests {
		card, err := conn.ZInterStore(test.key, []string{"z:inter:a", "z:inter:b", "z:inter:c"}, test.weights, test.aggregate)
		assert.Nil(err)
		assert.Equal(card, test.card)
		// ZRange.
		scoredValues, err := conn.ZRange(test.key, 0, -1, true)
		assert.Nil(err)
		assert.Length(scoredValues, test.card)
		for i, value := range scoredValues {
			assert.Equal(value.Score, test.expected(i))
		}
		scoredValues, err = conn.ZRange(test.key, 0, -1, false)
		assert.Nil(err)
		assert.Length(scoredValues, test.card)
		for _, value := range scoredValues {
			assert.Equal(value.Score, 0.0)
		}
		// ZRevRange.
		scoredValues, err = conn.ZRevRange(test.key, 0, -1, true)
		assert.Nil(err)
		assert.Length(scoredValues, test.card)
		for i, value := range scoredValues {
			assert.Equal(value.Score, test.expected(len(scoredValues)-1-i))
		}
		scoredValues, err = conn.ZRange(test.key, 0, -1, false)
		assert.Nil(err)
		assert.Length(scoredValues, test.card)
		for _, value := range scoredValues {
			assert.Equal(value.Score, 0.0)
		}
	}
}

func TestZRangeByLex(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.ZAdd("z:range:lex", 0, "a", 0, "b", 0, "c", 0, "d")
	conn.ZAdd("z:range:lex", 0, "e", 0, "f", 0, "g", 0, "h")

	values, err := conn.ZRangeByLex("z:range:lex", "-", "[d", 0, -1)
	assert.Nil(err)
	assert.Length(values, 4)
	values, err = conn.ZRangeByLex("z:range:lex", "-", "[d", 0, 2)
	assert.Nil(err)
	assert.Length(values, 2)
	values, err = conn.ZRangeByLex("z:range:lex", "-", "(d", 0, -1)
	assert.Nil(err)
	assert.Length(values, 3)
	values, err = conn.ZRangeByLex("z:range:lex", "-", "+", 0, -1)
	assert.Nil(err)
	assert.Length(values, 8)
	values, err = conn.ZRangeByLex("z:range:lex", "[bb", "+", 0, -1)
	assert.Nil(err)
	assert.Length(values, 6)
	values, err = conn.ZRangeByLex("z:range:lex", "[bb", "+", 0, -1)
	assert.Nil(err)
	assert.Length(values, 6)
	values, err = conn.ZRangeByLex("z:range:lex", "[bb", "(f", 0, -1)
	assert.Nil(err)
	assert.Length(values, 3)
	values, err = conn.ZRangeByLex("z:range:lex", "[i", "+", 0, -1)
	assert.Nil(err)
	assert.Length(values, 0)

	removed, err := conn.ZRemRangeByLex("z:range:lex", "[c", "(g")
	assert.Nil(err)
	assert.Equal(removed, 4)
	card, err := conn.ZCard("z:range:lex")
	assert.Nil(err)
	assert.Equal(card, 4)
}

func TestZRank(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.ZAdd("z:rank", 1, "a", 2, "b", 3, "c", 4, "d")
	conn.ZAdd("z:rank", 5, "e", 6, "f", 7, "g", 8, "h")

	rank, err := conn.ZRank("z:rank", "c")
	assert.Nil(err)
	assert.Equal(rank, 2)
	rank, err = conn.ZRank("z:rank", "i")
	assert.Nil(err)
	assert.Equal(rank, -1)

	removed, err := conn.ZRemRangeByRank("z:rank", 1, 4)
	assert.Nil(err)
	assert.Equal(removed, 4)
	card, err := conn.ZCard("z:rank")
	assert.Nil(err)
	assert.Equal(card, 4)
}

func TestZScore(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	conn.ZAdd("z:score", 1, "a", 2, "b", 3, "c", 4, "d")
	conn.ZAdd("z:score", 5, "e", 6, "f", 7, "g", 8, "h")

	scoredValues, err := conn.ZRangeByScore("z:score", 2, 7, redis.InclusiveBoth, true, 0, -1)
	assert.Nil(err)
	assert.Length(scoredValues, 6)
	assert.True(scoredValues[0].Score < scoredValues[1].Score)

	scoredValues, err = conn.ZRevRangeByScore("z:score", 7, 2, redis.InclusiveBoth, true, 0, -1)
	assert.Nil(err)
	assert.Length(scoredValues, 6)
	assert.True(scoredValues[0].Score > scoredValues[1].Score)

	removed, err := conn.ZRemRangeByScore("z:score", 7, 8, redis.InclusiveBoth)
	assert.Nil(err)
	assert.Equal(removed, 2)
	card, err := conn.ZCard("z:score")
	assert.Nil(err)
	assert.Equal(card, 6)
}

//--------------------
// TOOLS
//--------------------

// generateSortedSetData generates a sorted set of data at the given key.
func generateSortedSetData(assert asserts.Assertion, conn *redis.Connection, key, charset string) {
	svs := []interface{}{}
	for i := 1; i < 11; i++ {
		for _, c := range charset {
			v := fmt.Sprintf("%c%d", c, i)
			svs = append(svs, i, v)
		}
	}
	added, err := conn.ZAdd(key, svs...)
	assert.Nil(err)
	assert.Equal(added, len(svs)/2)
}

// EOF
