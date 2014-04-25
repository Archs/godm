// Tideland Go Data Management - Redis Client - Errors
//
// Copyright (C) 2009-2014 Frank Mueller / Tideland / Oldenburg / Germany
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

const (
	ErrInvalidConfiguration = iota
	ErrDatabaseClosed
	ErrConnection
	ErrUnexpectedRequest
	ErrUnexpectedReply
	ErrCommunication
	ErrServerResponse
	ErrKeyNotFound
	ErrFuture
	ErrReply
	ErrInvalidType
	ErrInvalidKey
	ErrTimeout
	ErrInvalidResponse
	ErrInvalidResultCount
)

var errorMessages = errors.Messages{
	ErrInvalidConfiguration: "invalid configuration value in field %q: %v",
	ErrDatabaseClosed:       "database closed",
	ErrConnection:           "cannot establish connection",
	ErrUnexpectedRequest:    "unexpected request: %v",
	ErrUnexpectedReply:      "unexpected reply: %v",
	ErrCommunication:        "cannot communicate with server: %v",
	ErrServerResponse:       "server responded error",
	ErrKeyNotFound:          "key not found",
	ErrFuture:               "invalid future result value: %v",
	ErrReply:                "invalid reply, length is %v",
	ErrInvalidType:          "invalid type conversion of \"%v\" to %q",
	ErrInvalidKey:           "invalid key %q",
	ErrTimeout:              "timeout waiting for the response after command %q",
	ErrInvalidResponse:      "invalid server response: %v",
	ErrInvalidResultCount:   "result count does not match: %d <> %d",
}

//--------------------
// ERROR
//--------------------

// IsInvalidConfigurationError checks for an invalid configuration error.
func IsInvalidConfigurationError(err error) bool {
	return errors.IsError(err, ErrInvalidConfiguration)
}

// IsDatabaseClosedError checks for an error signalling a closed database.
func IsDatabaseClosedError(err error) bool {
	return errors.IsError(err, ErrDatabaseClosed)
}

// IsConnectionError checks for a connection error.
func IsConnectionError(err error) bool {
	return errors.IsError(err, ErrConnection)
}

// IsCommunicationError checks for a communication error.
func IsCommunicationError(err error) bool {
	return errors.IsError(err, ErrCommunication)
}

// IsServerResponseError checks for an error signaled as response by
// the Redis server.
func IsServerResponseError(err error) bool {
	return errors.IsError(err, ErrServerResponse)
}

// IsKeyNotFoundError checks for an error responded by Redis
// when a key is not found.
func IsKeyNotFoundError(err error) bool {
	return errors.IsError(err, ErrKeyNotFound)
}

// IsFutureError checks for an error inside the future handling.
func IsFutureError(err error) bool {
	return errors.IsError(err, ErrFuture)
}

// IsReplyError checks for an error after an invalid reply.
func IsReplyError(err error) bool {
	return errors.IsError(err, ErrReply)
}

// IsInvalidTypeError checks for an error after an invalid type conversion.
func IsInvalidTypeError(err error) bool {
	return errors.IsError(err, ErrInvalidType)
}

// IsInvalidKeyError checks for an error when using an invalid key.
func IsInvalidKeyError(err error) bool {
	return errors.IsError(err, ErrInvalidKey)
}

// IsTimeoutError checks for a timeout error when waiting for a response.
func IsTimeoutError(err error) bool {
	return errors.IsError(err, ErrTimeout)
}

// IsInvalidResponseError checks for an error after an invalid server response.
func IsInvalidResponseError(err error) bool {
	return errors.IsError(err, ErrInvalidResponse)
}

// IsInvalidResultCountError checks for an error when a multi command
// returns with an invalid state.
func IsInvalidResultCountError(err error) bool {
	return errors.IsError(err, ErrInvalidResultCount)
}

// EOF
