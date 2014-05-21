// Tideland Go Data Management - Redis Client - Commands - Sorted Sets
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import (
	"strconv"

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// SORTED SET COMMANDS
//--------------------

// Aggregate defines the aggregation at ZInterStore and ZUnionStore.
type Aggregate int

// Values for the aggregation at ZInterStore and ZUnionStore.
const (
	AggregateSum Aggregate = iota
	AggregateMin
	AggregateMax
)

// Inclusive defines if min and max values are inclusive at ZRange commands.
type Inclusive int

// Values for inclusive at ZRange commands.
const (
	InclusiveNone Inclusive = iota
	InclusiveMin
	InclusiveMax
	InclusiveBoth
)

// ZAdd adds all the specified members with the specified scores to
// the sorted set stored at key. The caller is responsible to pass
// alternately numbers (ints or floats) and values.
func (conn *Connection) ZAdd(key string, svs ...interface{}) (int, error) {
	args := []interface{}{key}
	args = append(args, svs...)
	result, err := conn.Command("zadd", args...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// ZCard returns the sorted set cardinality (number of elements)
// of the sorted set stored at key.
func (conn *Connection) ZCard(key string) (int, error) {
	result, err := conn.Command("zcard", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// ZCount returns all the elements in the sorted set at key with a score
// between min and max.
func (conn *Connection) ZCount(key string, min, max float64, incl Inclusive) (int, error) {
	smin, smax := inclScore(min, max, incl)
	result, err := conn.Command("zcount", key, smin, smax)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// ZIncrBy increments the score of member in the sorted set stored
// at key by increment.
func (conn *Connection) ZIncrBy(key string, incr float64, member interface{}) (string, error) {
	result, err := conn.Command("zincrby", key, incr, member)
	if err != nil {
		return "", err
	}
	return result.StringAt(0)
}

// ZInterStore computes the intersection of numkeys sorted sets given
// by the specified keys, and stores the result in destination
func (conn *Connection) ZInterStore(destination string, keys []string, weights []float64, aggregate Aggregate) (int, error) {
	if weights != nil && len(keys) != len(weights) {
		return 0, errors.New(ErrDivergentKeyWeightLen, errorMessages)
	}
	args := []interface{}{destination, len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	if weights != nil {
		args = append(args, "weights")
		for _, weight := range weights {
			args = append(args, weight)
		}
	}
	switch aggregate {
	case AggregateSum:
		args = append(args, "aggregate", "sum")
	case AggregateMin:
		args = append(args, "aggregate", "min")
	case AggregateMax:
		args = append(args, "aggregate", "max")
	}
	result, err := conn.Command("zinterstore", args...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// ZRange returns the specified range of elements in the sorted set stored
// at key.
func (conn *Connection) ZRange(key string, start, stop int, withscores bool) (ScoredValues, error) {
	args := []interface{}{key, start, stop}
	if withscores {
		args = append(args, "withscores")
	}
	result, err := conn.Command("zrange", args...)
	if err != nil {
		return nil, err
	}
	return result.ScoredValues(withscores)
}

// ZRangeByLex returns all the elements in the sorted set at key with a value between min and max
// when all the elements in a sorted set are inserted with the same score, in order to force
// lexicographical ordering. If the elements in the sorted set have different scores, the
// returned elements are unspecified.
func (conn *Connection) ZRangeByLex(key, min, max string, offset, count int) ([]Value, error) {
	args := []interface{}{key, min, max}
	if count > -1 {
		args = append(args, "limit", offset, count)
	}
	result, err := conn.Command("zrangebylex", args...)
	if err != nil {
		return nil, err
	}
	return result.Values(), nil
}

// ZRangeByScore returns all the elements in the sorted set at key with a score between min and max
// (including elements with score equal to min or max). The elements are considered to be ordered from
// low to high scores.
func (conn *Connection) ZRangeByScore(key string, min, max float64, incl Inclusive, withscores bool, offset, count int) (ScoredValues, error) {
	smin, smax := inclScore(min, max, incl)
	args := []interface{}{key, smin, smax}
	if withscores {
		args = append(args, "withscores")
	}
	if count > -1 {
		args = append(args, "limit", offset, count)
	}
	result, err := conn.Command("zrangebyscore", args...)
	if err != nil {
		return nil, err
	}
	return result.ScoredValues(withscores)
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from
// low to high. The rank (or index) is 0-based, which means that the member with the lowest score
// has rank 0.
func (conn *Connection) ZRank(key string, member interface{}) (int, error) {
	result, err := conn.Command("zrank", key, member)
	if err != nil {
		return -1, err
	}
	value, err := result.ValueAt(0)
	if err != nil {
		return -1, err
	}
	if value.String() == "(nil)" {
		return -1, nil
	}
	return value.Int()
}

// ZRem removes rthe specified members from the sorted set stored at key.
func (conn *Connection) ZRem(key string, members ...interface{}) (int, error) {
	args := []interface{}{key}
	args = append(args, members...)
	result, err := conn.Command("zrem", args...)
	if err != nil {
		return -1, err
	}
	return result.IntAt(0)
}

// ZRemRangeByLex removes all elements in the sorted set stored at key between
// the lexicographical range specified by min and max, when all the elements
// in a sorted set are inserted with the same score.
func (conn *Connection) ZRemRangeByLex(key, min, max string) (int, error) {
	result, err := conn.Command("zremrangebylex", key, min, max)
	if err != nil {
		return -1, err
	}
	return result.IntAt(0)
}

// ZRemRangeByRank removes all elements in the sorted set stored at key with
// rank between start and stop.
func (conn *Connection) ZRemRangeByRank(key string, start, stop int) (int, error) {
	result, err := conn.Command("zremrangebyrank", key, start, stop)
	if err != nil {
		return -1, err
	}
	return result.IntAt(0)
}

// ZRemRangeByScore removes all elements in the sorted set stored at key
// with a score between min and max (inclusive).
func (conn *Connection) ZRemRangeByScore(key string, min, max float64, incl Inclusive) (int, error) {
	smin, smax := inclScore(min, max, incl)
	result, err := conn.Command("zremrangebyscore", key, smin, smax)
	if err != nil {
		return -1, err
	}
	return result.IntAt(0)
}

// ZRevRange returns the specified range of elements in the sorted set stored
// at key in a reverse order.
func (conn *Connection) ZRevRange(key string, start, stop int, withscores bool) (ScoredValues, error) {
	args := []interface{}{key, start, stop}
	if withscores {
		args = append(args, "withscores")
	}
	result, err := conn.Command("zrevrange", args...)
	if err != nil {
		return nil, err
	}
	return result.ScoredValues(withscores)
}

// ZRevRangeByScore returns all the elements in the sorted set at key with a score between min and max
// (including elements with score equal to min or max). The elements are considered to be ordered from
// high to low scores.
func (conn *Connection) ZRevRangeByScore(key string, max, min float64, incl Inclusive, withscores bool, offset, count int) (ScoredValues, error) {
	smax, smin := inclScore(max, min, incl)
	args := []interface{}{key, smax, smin}
	if withscores {
		args = append(args, "withscores")
	}
	if count > -1 {
		args = append(args, "limit", offset, count)
	}
	result, err := conn.Command("zrevrangebyscore", args...)
	if err != nil {
		return nil, err
	}
	return result.ScoredValues(withscores)
}

//--------------------
// HELPER
//--------------------

// inclScore returns the scores inclusive their possible inclusive prefixes.
func inclScore(min, max float64, incl Inclusive) (string, string) {
	minIncl := incl == InclusiveMin || incl == InclusiveBoth
	maxIncl := incl == InclusiveMax || incl == InclusiveBoth
	scorer := func(value float64, valueIncl bool) string {
		s := strconv.FormatFloat(value, 'f', -1, 64)
		if !valueIncl {
			s = "(" + s
		}
		return s
	}
	return scorer(min, minIncl), scorer(max, maxIncl)
}

// EOF
