// Tideland Go Data Management - Cache - Unit Test
//
// Copyright (C) 2009-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package cache_test

//--------------------
// IMPORTS
//--------------------

import (
	"fmt"
	"testing"
	"time"

	"github.com/tideland/godm/v2/cache"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

// Test the normal retrieving without errors.
func TestNormalRetrieve(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	// Environment.
	ctr := 0
	count := func() (interface{}, error) {
		ctr++
		return ctr, nil
	}
	cv := cache.NewCachedValue(count, 25*time.Millisecond)
	defer cv.Remove()
	retrieve := func() int { v, _ := cv.Value(); return v.(int) }

	// Asserts.
	assert.Equal(retrieve(), 1)
	assert.Equal(retrieve(), 1)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(retrieve(), 2)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(retrieve(), 3)
	assert.Equal(retrieve(), 3)
}

// Test the retrieving with an error.
func TestRetrieveError(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	// Environment.
	ctr := 0
	efunc := func() (interface{}, error) {
		ctr++
		return nil, fmt.Errorf("ouch %d", ctr)
	}
	cv := cache.NewCachedValue(efunc, 25*time.Millisecond)
	defer cv.Remove()
	retrieve := func() error { _, err := cv.Value(); return err }

	// Asserts.
	assert.ErrorMatch(retrieve(), "ouch 1")
	assert.ErrorMatch(retrieve(), "ouch 2")
	assert.ErrorMatch(retrieve(), "ouch 3")
}

// Test the retrieving with a panic.
func TestRetrievePanic(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)

	// Environment.
	ctr := 0
	pfunc := func() (interface{}, error) {
		ctr++
		panic(fmt.Sprintf("ouch %d", ctr))
	}
	cv := cache.NewCachedValue(pfunc, 25*time.Millisecond)
	defer cv.Remove()
	retrieve := func() error { _, err := cv.Value(); return err }

	// Asserts.
	assert.ErrorMatch(retrieve(), `\[E.*\] cannot retrieve cached value: ouch 1`)
	assert.ErrorMatch(retrieve(), `\[E.*\] cannot retrieve cached value: ouch 2`)
	assert.ErrorMatch(retrieve(), `\[E.*\] cannot retrieve cached value: ouch 3`)
}

// EOF
