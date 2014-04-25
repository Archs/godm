// Tideland Go Data Management - Write-once / Read-multiple
//
// Copyright (C) 2012-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

// WORM contains some helpful collection types which only can
// be written once but read multiple. So they can be shared
// between goroutines without the risk of modification while
// processing.
package worm

// EOF
