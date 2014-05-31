// Tideland Go Data Management - Redis Client
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

// A very powerful as well as convenient client for accessing the
// Redis database.
//
// After opening the database with Open() a pooled connection can be
// retrieved using db.Connection(). It has be returnded to the pool with
// conn.Return(), optimally done using a defer after retrieving. The
// connection provides a Do() method to execute any command. It returns
// a result set with helpers to access the returned values and convert
// them into Go types. For typical returnings there are DoXxx() methods.
// Additionally a subscription can be retrieved with db.Subscription().
// Here channels can be subscribed or unsubscribed and retrieved using
// a channel. If it's not needed anymore the subscription can be closed
// using sub.Close().
package redis

// EOF
