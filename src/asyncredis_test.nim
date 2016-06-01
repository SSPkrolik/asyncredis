import asyncdispatch
import asyncredis
import strutils
import tables
import times
import unittest

let ar = newAsyncRedis("localhost", poolSize=1)
discard waitFor(ar.connect())

suite "Async Redis Client testing":

    test "Test constructor":
        check(ar != nil)

    test "COMMAND: FLUSHALL":
        check(waitFor(ar.FLUSHALL()).success)

    test "COMMAND: FLUSHDB":
        check(waitFor(ar.FLUSHDB()).success)

    test "COMMAND: APPEND":
        try:
            check(waitFor(ar.APPEND("string", "world")) mod 5 == 0)
        except UnsupportedError:
            discard

    test "COMMAND: AUTH":
        check(waitFor(ar.AUTH("super-password")).success == false)

    test "COMMAND: BGREWRITEAOF":
        let resp = waitFor(ar.BGREWRITEAOF())
        if not resp.success:
            check("in progress" in resp.message)
        else:
            check(resp.success)

    test "COMMAND: BGSAVE":
        let resp = waitFor(ar.BGSAVE())
        if not resp.success:
            check("in progress" in resp.message)
        else:
            check(resp.success)

    test "COMMAND: BITCOUNT":
        try:
            check(waitFor(ar.SET("hello", "world")))
            check(waitFor(ar.BITCOUNT("hello")) == 23)
            check(waitFor(ar.BITCOUNT("hello", 1, 1)) == 6)
        except UnsupportedError:
            discard

    test "COMMAND: CLIENT GETNAME":
        try:
            check(waitFor(ar.CLIENT_GETNAME()) == nil)
        except UnsupportedError:
            discard

    test "COMMAND: CLIENT LIST":
        try:
            check(waitFor(ar.CLIENT_LIST()).len() >= 1)
        except UnsupportedError:
            discard

    test "COMMAND: DBSIZE":
        check(waitFor(ar.DBSIZE()) > 0)

    test "COMMAND: DEL":
        check(waitFor(ar.DEL("non-existing-key")) == 0)
        check(waitFor(ar.DEL(@["nxk1", "nxk2"])) == 0)

    test "COMMAND: DUMP":
        try:
            check(waitFor(ar.DUMP("hello")).len() == 17)
        except UnsupportedError:
            discard

    test "COMMAND: ECHO":
        check(waitFor(ar.ECHO("hello")) == "hello")

    test "COMMAND: INFO":
        check(waitFor(ar.INFO()).hasKey("redis_version"))

    test "COMMAND: PING":
        check(waitFor(ar.PING())== true)

    test "COMMAND: SET":
        check(waitFor(ar.SET("string", "world")) == true)
        check(waitFor(ar.SET("int", 5)) == true)
        check(waitFor(ar.SET("float", 5.5)) == true)

    test "COMMAND: GET":
        check(waitFor(ar.GET("string")) == "world")
        check(waitFor(ar.GET("int")) == "5")
        check(waitFor(ar.GET("float")) == "5.5")
        check(waitFor(ar.GET("non-existing-key")) == nil)

    test "COMMAND: TIME":
        try:
            check(timeInfoToTime(waitFor(ar.TIME())).toSeconds() > 0)
        except UnsupportedError:
            discard

    test "COMMAND: TTL":
        check(waitFor(ar.TTL("non-existing-key")) == ttlDoesNotExist)
        check(waitFor(ar.TTL("hello")) == ttlInfinite)