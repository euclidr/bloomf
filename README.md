Bloomf Bloomfilter written in Go, backed by redis
-------------------------------------------------
[![Build Status](https://travis-ci.com/euclidr/bloomf.svg?branch=master)](https://travis-ci.com/euclidr/bloomf)
[![codecov](https://codecov.io/gh/euclidr/bloomf/branch/master/graph/badge.svg)](https://codecov.io/gh/euclidr/bloomf)
[![GoDoc](https://godoc.org/github.com/euclidr/bloomf?status.svg)](https://godoc.org/github.com/euclidr/bloomf)

Bloomf is a Bloomfilter written in Go and backed by redis. 

* It makes use of several redis bitmap, so It won't be limited to 512MB which is the max of redis bitmap. 
* Every `add` and `check` only take one roundtrip for `pipeline` is used. 
* It stores metadata in redis, you can fetch the metadata and restore bloomfilter instance, so Apps that use bloomf can be stateless.
* It use murmur3 hash function which will be uniformly distributed and high performance.

## Getting started

### Getting bloomf

```
$ go get github.com/euclidr/bloomf
```

### dependencies

go packages bellow is also needed

* [go-redis](https://github.com/go-redis/redis)
* [murmur3](https://github.com/twmb/murmur3)


### Example

```go
package main

import (
    "github.com/euclidr/bloomf"
    "github.com/go-redis/redis"
)


func main() {
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    bl, err := bloomf.New(client, "bf" 1000000, 0.001)
    if err == bloomf.ErrDuplicated {
        bl, _ := bloomf.GetByName(client, "bf")
    }

    bl.Add([]bytes("awesome key"))

    exists, _ := bl.Exists([]bytes("awesome key"))
    if exists {
        ...
    }

    ...

    // bl.Clear() // you can clean up all datas in redis
}
```