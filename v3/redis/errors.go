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
	ErrConnectionClosed
	ErrConnectionsInUse
	ErrConnectionEstablishing
	ErrUnexpectedRequest
	ErrUnexpectedReply
	ErrCommunication
	ErrServerResponse
	ErrKeyNotFound
	ErrCannotSetKey
	ErrCannotRenameKey
	ErrCannotSetList
	ErrCannotTrimList
	ErrReply
	ErrInvalidType
	ErrInvalidKey
	ErrTimeout
	ErrInvalidResponse
	ErrInvalidResultCount
	ErrIllegalItemIndex
	ErrIllegalItemType
)

var errorMessages = errors.Messages{
	ErrInvalidConfiguration:   "invalid configuration value in field %q: %v",
	ErrConnectionClosed:       "connection closed",
	ErrConnectionsInUse:       "all %d configured connections in use",
	ErrConnectionEstablishing: "cannot establish connection",
	ErrUnexpectedRequest:      "unexpected request: %v",
	ErrUnexpectedReply:        "unexpected reply: %v",
	ErrCommunication:          "cannot communicate with server: %v",
	ErrServerResponse:         "server responded error: %v",
	ErrKeyNotFound:            "key %q not found",
	ErrCannotSetKey:           "cannot set key %q",
	ErrCannotRenameKey:        "cannot rename key %q",
	ErrCannotSetList:          "cannot set list %q at index %d",
	ErrCannotTrimList:         "cannot trim list %q between %d and %d",
	ErrReply:                  "invalid reply, length is %v",
	ErrInvalidType:            "invalid type conversion of \"%v\" to %q: %v",
	ErrInvalidKey:             "invalid key %q",
	ErrTimeout:                "timeout waiting for the response after command %q",
	ErrInvalidResponse:        "invalid server response: %v",
	ErrInvalidResultCount:     "result count does not match: %d <> %d",
	ErrIllegalItemIndex:       "item index %d is illegal for result set size %d",
	ErrIllegalItemType:        "item at index %d is no %s",
}

// EOF
