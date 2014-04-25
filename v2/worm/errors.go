// Tideland Go Data Management - Write once read multiple - Errors
//
// Copyright (C) 2012-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package worm

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
	ErrInvalidKey = iota
	ErrInvalidType
)

var errorMessages = errors.Messages{
	ErrInvalidKey:  "invalid key %q for the dictionary",
	ErrInvalidType: "invalid type %q expected for key %q",
}

//--------------------
// ERRORS
//--------------------

// IsInvalidKeyError tests the error type.
func IsInvalidKeyError(err error) bool {
	return errors.IsError(err, ErrInvalidKey)
}

// IsInvalidTypeError tests the error type.
func IsInvalidTypeError(err error) bool {
	return errors.IsError(err, ErrInvalidType)
}

// EOF
