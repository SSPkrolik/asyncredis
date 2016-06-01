# asyncredis

Asynchronous Redis client for Nim that uses Nim `asyncdispatch` Nim's asyncronous
I/O tooling. The client impements connection pool exactly the same way as `nimongo` does.

## Installation

```bash
$ nimble install https://github.com/SSPkrolik/asyncredis.git
```

## Usage

TODO

## Implementation Status

| Command        | Status             |                   |
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
| CLIENT GETNAME, KILL, LIST, PAUSE, REPLY, SETNAME | :white_check_mark: |                   |
| DBSIZE         | :white_check_mark: |                   |
| DEL            | :white_check_mark: |                   |
| DUMP           | :white_check_mark: |                   |
| ECHO           | :white_check_mark: |                   |
| FLUSHALL       | :white_check_mark: |                   |
| FLUSHDB        | :white_check_mark: |                   |
| GET            | :white_check_mark: |                   |
| PING           | :white_check_mark: |                   |
| SET            | :white_check_mark: | No TTL            |
| TIME           | :white_check_mark: |                   |
| TTL            | :white_check_mark: |                   |