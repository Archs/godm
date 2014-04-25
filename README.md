Tideland Go Data Management
===========================

Description
-----------

The *Tideland Go Data Management* (GODM) is a number of package for the
management and processing of data:

- Cache provides a simple caching of lazy loaded values,
- Map/Reduce allows you process larger amounts of data using the
  map reduce algorithm,
- Numerics contains functions for statistical analyzis,
- Redis Client provides access to the Redis NoSQL database,
- Simple Markup Language is a markup language in the style of LISP,
  only with curly braces,
- Sort is a parallel working Quicksort and
- Write-once / Read-multiple contains a number of data structures where
  data can be written only once but read multiple times concurrently
  by different goroutines.

Installation
------------

    go get github.com/tideland/godm/v2/cache
    go get github.com/tideland/godm/v2/mapreduce
    go get github.com/tideland/godm/v2/numerics
    go get github.com/tideland/godm/v2/redis
    go get github.com/tideland/godm/v2/sml
    go get github.com/tideland/godm/v2/sort
    go get github.com/tideland/godm/v2/worm

Usage
-----

**Cache**

The cache package provides a caching for individual lazy loaded values.
An own retrieval function and a time to live (ttl) for the cached value
have to be passed. It will be retrieved with the first access to the
value and will be removed if the ttl has been exceeded. The next access
will retrieve it again.

**Map/Reduce**

Map/Reduce is an algorithm for the processing and aggregating mass data.
A type implementing the `MapReducer` interface has to be implemented and
passed to the `MapReduce()` function. The type is responsible for the
input, the mapping, the reducing and the consuming while the package
provides the runtime environment for it.

**Numerics**

Numerics is a mathematical package with points and vectors as types and
functions for the evaluation of polynomal, cubic spline and least squares
functions.

**Redis Client**

A database connection is established with

    db, err := redis.Connect(configuration)

The configuration defines stuff like the address, the database,
authentication and more. See the documentation. Passing nil connects to
an unauthenticated database 0 on localhost via TCP/IP. Now commands can
be executed with

    rs, err := db.Command("get", "foo")

Here `rs` is a result set containing the returned values with comfortable
accessor methods. The arguments after the command will be serialized in a
flexible way so that the commands are supported in an optimal way. See
redis.Hash or the interface redis.Hashable in the documentation.

Other functions support the execution of multi-commands, subscriptions
and publishings.

### Simple Markup Language

The simple markup language is a LISP like language looking like this:

    {foo
        {bar:1 Lorem ipsum ...}
        {bar:2 Foo bar ...}
        {bar:3
            Yadda {strong yadda} yadda.
        }
        {! Raw node can contain { and }. !}
    }

The package provides readers, writers and a data structure for these
documents. When writing a document a context has to be created where
different processors for individual tags can be registered. SML and XML
writer processors are included.

**Sort**

Sort takes instances implementing the Go `sort.Interface` interface. A call
of `sort.Sort(mySortable)` works like the Go sort and sorts the instance
using a parallel working quicksrt.

**Write-once / Read-multiple**

The WORM package contains several types to store ints and strings in lists,
sets and maps as well as bools in maps. Once a type is constructed with an
initial set of values it canot be changed. Aplying new values returns a
new instance instead.

All types provide several methods for accessing, testing and exporting.

And now have fun. ;)

Documentation
-------------

- http://godoc.org/github.com/tideland/godm/v2/cache
- http://godoc.org/github.com/tideland/godm/v2/mapreduce
- http://godoc.org/github.com/tideland/godm/v2/numerics
- http://godoc.org/github.com/tideland/godm/v2/redis
- http://godoc.org/github.com/tideland/godm/v2/sml
- http://godoc.org/github.com/tideland/godm/v2/sort
- http://godoc.org/github.com/tideland/godm/v2/worm

Authors
-------

- Frank Mueller - <mue@tideland.biz>
- Alex Browne - <stephenalexbrowne@gmail.com> (Redis Unix socket)

License
-------

*Tideland Go Data Management* is distributed under the terms of the BSD 3-Clause license.
