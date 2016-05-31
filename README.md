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

| Command      | Status             | Notes             |
|-------------:|:-------------------|:------------------|
| APPEND       | :white_check_mark: |                   |
| AUTH         | :white_check_mark: |                   | 
| BGREWRITEAOF | :white_check_mark: |                   |
| BGSAVE       | :white_check_mark: |                   |
| BITCOUNT     | :white_check_mark: |                   |
| BLPOP        | :red_circle:       |                   |
| GET          | :white_check_mark: |                   |
| PING         | :white_check_mark: |                   |
| SET          | :white_check_mark: | No TTL            |