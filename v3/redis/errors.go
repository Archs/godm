// Tideland Go Data Management - Redis Client - Errors
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
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// CONSTANTS
//--------------------

// Error codes.
const (
	ErrInvalidConfiguration = iota
	ErrPoolLimitReached
	ErrConnectionEstablishing
	ErrConnectionBroken
	ErrInvalidResponse
	ErrServerResponse
	ErrTimeout
	ErrAuthenticate
	ErrSelectDatabase
	ErrUseSubscription
	ErrInvalidType
	ErrInvalidKey
	ErrIllegalItemIndex
	ErrIllegalItemType
)

var errorMessages = errors.Messages{
	ErrInvalidConfiguration:   "invalid configuration value in field %q: %v",
	ErrPoolLimitReached:       "connection pool limit reached",
	ErrConnectionEstablishing: "cannot establish connection",
	ErrConnectionBroken:       "connection is broken",
	ErrInvalidResponse:        "invalid server response: %q",
	ErrServerResponse:         "server responded error: %v",
	ErrTimeout:                "timeout waiting for the response after command %q",
	ErrAuthenticate:           "cannot authenticate",
	ErrSelectDatabase:         "cannot select database",
	ErrUseSubscription:        "use subscription type for subscriptions",
	ErrInvalidType:            "invalid type conversion of \"%v\" to %q",
	ErrInvalidKey:             "invalid key %q",
	ErrIllegalItemIndex:       "item index %d is illegal for result set size %d",
	ErrIllegalItemType:        "item at index %d is no %s",
}

// EOF
