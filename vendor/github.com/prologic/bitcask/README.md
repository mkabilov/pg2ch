# bitcask

[![Build Status](https://cloud.drone.io/api/badges/prologic/bitcask/status.svg)](https://cloud.drone.io/prologic/bitcask)
[![CodeCov](https://codecov.io/gh/prologic/bitcask/branch/master/graph/badge.svg)](https://codecov.io/gh/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/prologic/bitcask)](https://goreportcard.com/report/prologic/bitcask)
[![GoDoc](https://godoc.org/github.com/prologic/bitcask?status.svg)](https://godoc.org/github.com/prologic/bitcask) 
[![Sourcegraph](https://sourcegraph.com/github.com/prologic/bitcask/-/badge.svg)](https://sourcegraph.com/github.com/prologic/bitcask?badge)
[![GitHub license](https://img.shields.io/github/license/prologic/bitcask.svg)](https://github.com/prologic/bitcask)
[![Github all releases](https://img.shields.io/github/downloads/prologic/bitcask/total.svg)](https://github.com/prologic/bitcask/releases)

A high performance Key/Value store written in [Go](https://golang.org) with a predictable read/write performance and high throughput. Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/).

For a more feature-complete Redis-compatible server, distributed key/value store have a look at [Bitraft](https://github.com/prologic/bitraft) which uses this library as its backend. Use [Bitcask](https://github.com/prologic/bitcask) as a starting point or if you want to embed in your application, use [Bitraft](https://github.com/prologic/bitraft) if you need a complete server/client solution with high availability with a Redis-compatible API.

## Features

* Embeddable (`import "github.com/prologic/bitcask"`)
* Builtin CLI (`bitcask`)
* Builtin Redis-compatible server (`bitcaskd`)
* Predictable read/write performance
* Low latecny
* High throughput (See: [Performance](README.md#Performance)

## Development

1. Get the source:

```#!bash
$ git clone https://github.com/prologic/bitcask.git
```

2. Install required tools:

This library uses [Protobuf](https://github.com/protocolbuffers/protobuf) to serialize data on disk. Please follow the
instructions for installing `protobuf` on your system. You will also need the
following Go libraries/tools to generate Go code from Protobuf defs:

3. Build the project:

```#!bash
$ make
```

This will invoke `go generate` and `go build`.

- [protoc-gen-go](https://github.com/golang/protobuf)

## Install

```#!bash
$ go get github.com/prologic/bitcask
```

## Usage (library)

Install the package into your project:

```#!bash
$ go get github.com/prologic/bitcask
```

```#!go
package main

import "github.com/prologic/bitcask"

func main() {
    db, _ := bitcask.Open("/tmp/db")
    defer db.Close()
    db.Set("Hello", []byte("World"))
    val, _ := db.Get("hello")
}
```

See the [godoc](https://godoc.org/github.com/prologic/bitcask) for further
documentation and other examples.

## Usage (tool)

```#!bash
$ bitcask -p /tmp/db set Hello World
$ bitcask -p /tmp/db get Hello
World
```

## Usage (server)

There is also a builtin very  simple Redis-compatible server called `bitcaskd`:

```#!bash
$ ./bitcaskd ./tmp
INFO[0000] starting bitcaskd v0.0.7@146f777              bind=":6379" path=./tmp
```

Example session:

```
$ telnet localhost 6379
Trying ::1...
Connected to localhost.
Escape character is '^]'.
SET foo bar
+OK
GET foo
$3
bar
DEL foo
:1
GET foo
$-1
PING
+PONG
QUIT
+OK
Connection closed by foreign host.
```

## Docker

You can also use the [Bitcask Docker Image](https://cloud.docker.com/u/prologic/repository/docker/prologic/bitcask):

```#!bash
$ docker pull prologic/bitcask
$ docker run -d -p 6379:6379 prologic/bitcask
```

## Performance

Benchmarks run on a 11" Macbook with a 1.4Ghz Intel Core i7:

```
$ make bench
...
BenchmarkGet/128B-4         	  500000	      2537 ns/op	     672 B/op	       7 allocs/op
BenchmarkGet/256B-4         	  500000	      2629 ns/op	    1056 B/op	       7 allocs/op
BenchmarkGet/512B-4         	  500000	      2773 ns/op	    1888 B/op	       7 allocs/op
BenchmarkGet/1K-4           	  500000	      3202 ns/op	    3552 B/op	       7 allocs/op
BenchmarkGet/2K-4           	  300000	      3904 ns/op	    6880 B/op	       7 allocs/op
BenchmarkGet/4K-4           	  300000	      5678 ns/op	   14048 B/op	       7 allocs/op
BenchmarkGet/8K-4           	  200000	      8948 ns/op	   27360 B/op	       7 allocs/op
BenchmarkGet/16K-4          	  100000	     14635 ns/op	   53472 B/op	       7 allocs/op
BenchmarkGet/32K-4          	   50000	     28292 ns/op	  114912 B/op	       7 allocs/op

BenchmarkPut/128B-4         	  200000	      8173 ns/op	     409 B/op	       6 allocs/op
BenchmarkPut/256B-4         	  200000	      8404 ns/op	     538 B/op	       6 allocs/op
BenchmarkPut/512B-4         	  200000	      9741 ns/op	     829 B/op	       6 allocs/op
BenchmarkPut/1K-4           	  100000	     13118 ns/op	    1411 B/op	       6 allocs/op
BenchmarkPut/2K-4           	  100000	     17982 ns/op	    2573 B/op	       6 allocs/op
BenchmarkPut/4K-4           	   50000	     35477 ns/op	    5154 B/op	       6 allocs/op
BenchmarkPut/8K-4           	   30000	     54021 ns/op	    9804 B/op	       6 allocs/op
BenchmarkPut/16K-4          	   20000	     96551 ns/op	   18849 B/op	       6 allocs/op
BenchmarkPut/32K-4          	   10000	    129957 ns/op	   41561 B/op	       7 allocs/op

BenchmarkScan-4             	 1000000	      2011 ns/op	     493 B/op	      25 allocs/op
```

For 128B values:

* ~400,000 reads/sec
* ~130,000 writes/sec

The full benchmark above shows linear performance as you increase key/value sizes.

## License

bitcask is licensed under the [MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)
