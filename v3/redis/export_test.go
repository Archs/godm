// Tideland Go Data Management - Redis Client - Unit Tests - Export
//
// Copyright (C) 2009-2013 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import ()

//--------------------
// EXPORTS
//--------------------

var NewResultSet = newResultSet
var NewPublishedValues = newPublishedValues

// AppendValue appends a value to the passed result set.
func AppendValue(rs *ResultSet, value interface{}) {
	rs.append(NewValue(value))
}

// EOF
