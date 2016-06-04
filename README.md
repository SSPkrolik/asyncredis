# asyncredis

Asynchronous Redis client for Nim that uses Nim `asyncdispatch` Nim's asyncronous
I/O tooling. The client impements connection pool exactly the same way as `nimongo` does.

## Installation

```bash
$ nimble install https://github.com/SSPkrolik/asyncredis.git
```

## Usage

First, create asynchronous Redis client, and perform connections establishment.
All connections from pool try to connect to Redis server.

```nim
import asyncredis

let ar = newAsyncRedis("localhost", poolSize=1)
discard waitFor(ar.connect())
```

Asynchronous Redis client performs automated reconnection if disconnection
happened during certain command execution.

```nim
proc makeGetSet(): Future[string] {.async.} =
    discard await ar.SET("key", "value")
    let val = await ar.GET("key")
    return val

let res = waitFor(makeGetSet)
echo res
```

## Implementation Status

| Command        | Status             | Notes             |
|---------------:|:-------------------|:------------------|
| APPEND         | :white_check_mark: |                   |
| AUTH           | :white_check_mark: |                   | 
| BGREWRITEAOF   | :white_check_mark: |                   |
| BGSAVE         | :white_check_mark: |                   |
| BITCOUNT       | :white_check_mark: |                   |
| BITFIELD       | :red_circle:       |                   |
| BITOP          | :white_check_mark: |                   |
| BITPOS         | :red_circle:       |                   |
| BLPOP          | :red_circle:       |                   |
| BRPOP          | :red_circle:       |                   |
| BRPOPLPUSH     | :red_circle:       |                   |
| CLIENT GETNAME | :white_check_mark: |                   |
| CLIENT KILL    | :white_check_mark: |                   |
| CLIENT LIST    | :white_check_mark: |                   |
| CLIENT PAUSE   | :white_check_mark: |                   |
| CLIENT REPLY   | :white_check_mark: |                   |
| CLIENT SETNAME | :white_check_mark: |                   |
| COMMAND        | :red_circle:       |                   |
| COMMAND COUNT  | :white_check_mark: |                   |
| DBSIZE         | :white_check_mark: |                   |
| DEBUG OBJECT   | :white_check_mark: |                   |
| DEBUG SEGFAULT | :white_check_mark: |                   |
| DECR           | :white_check_mark: |                   |
| DECRBY         | :white_check_mark: |                   |
| DEL            | :white_check_mark: |                   |
| DISCARD        | :white_check_mark: | Transaction       |
| DUMP           | :white_check_mark: |                   |
| ECHO           | :white_check_mark: |                   |
| FLUSHALL       | :white_check_mark: |                   |
| FLUSHDB        | :white_check_mark: |                   |
| GET            | :white_check_mark: |                   |
| KEYS           | :white_check_mark: |                   |
| INCR           | :white_check_mark: |                   |
| INCRBY         | :white_check_mark: |                   |
| INCRBYFLOAT    | :white_check_mark: |                   |
| LASTSAVE       | :white_check_mark: |                   |
| LPUSH          | :white_check_mark: |                   |
| LPUSHX         | :white_check_mark: |                   |
| PING           | :white_check_mark: |                   |
| SET            | :white_check_mark: | No TTL            |
| TIME           | :white_check_mark: |                   |
| TTL            | :white_check_mark: |                   |
| TYPE           | :white_check_mark: |                   |
