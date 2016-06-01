import asyncdispatch
import asyncredis
import strutils
import tables
import unittest

let ar = newAsyncRedis("localhost")
discard waitFor(ar.connect())

suite "Async Redis Client testing":

    test "Test constructor":
        check(ar != nil)

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

    test "COMMAND: INFO":
        check(waitFor(ar.INFO()).hasKey("redis_version"))

    test "Command: PING":
        check(waitFor(ar.PING())== true)

    test "Command: SET":
        check(waitFor(ar.SET("string", "world")) == true)
        check(waitFor(ar.SET("int", 5)) == true)
        check(waitFor(ar.SET("float", 5.5)) == true)

    test "Command: GET":
        check(waitFor(ar.GET("string")) == "world")
        check(waitFor(ar.GET("int")) == "5")
        check(waitFor(ar.GET("float")) == "5.5")
        check(waitFor(ar.GET("non-existing-key")) == nil)
