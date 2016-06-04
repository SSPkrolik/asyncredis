import asyncdispatch
import asyncredis
import strutils
import tables
import times
import unittest

let ar = newAsyncRedis("localhost", poolSize=1)
require waitFor(ar.connect())

suite "Async Redis Client testing":

    test "Test constructor":
        check: ar != nil

    test "COMMAND: FLUSHALL":
        check(waitFor(ar.FLUSHALL()).success)

    test "COMMAND: FLUSHDB":
        check(waitFor(ar.FLUSHDB()).success)

    test "COMMAND: APPEND":
        try:
            check: waitFor(ar.APPEND("string", "world")) mod 5 == 0
        except UnsupportedError:
            skip()

    test "COMMAND: AUTH":
        check: waitFor(ar.AUTH("super-password")).success == false

    test "COMMAND: BGREWRITEAOF":
        let resp = waitFor(ar.BGREWRITEAOF())
        if not resp.success:
            check: "in progress" in resp.message
        else:
            check: resp.success

    test "COMMAND: BGSAVE":
        let resp = waitFor(ar.BGSAVE())
        if not resp.success:
            check: "in progress" in resp.message
        else:
            check: resp.success

    test "COMMAND: BITCOUNT":
        try:
            check:
                waitFor(ar.SET("hello", "world"))
                waitFor(ar.BITCOUNT("hello")) == 23
                waitFor(ar.BITCOUNT("hello", 1, 1)) == 6
        except UnsupportedError:
            skip()

    test "COMMAND: BITOP":
        discard waitFor(ar.SET("x", "x"))
        discard waitFor(ar.SET("y", "y"))
        try:
            check:
                waitFor(ar.BITOP(BitOperation.AND, "z", @["x", "y"])) == 1
                waitFor(ar.BITOP(BitOperation.OR, "z", @["x", "y"])) == 1
                waitFor(ar.BITOP(BitOperation.XOR, "z", @["x", "y"])) == 1
                waitFor(ar.BITOP(BitOperation.NOT, "z", @["x"])) == 1
            expect AssertionError:
                check: waitFor(ar.BITOP(BitOperation.NOT, "z", @["x", "y"])) == 1
        except UnsupportedError:
            skip()

    test "COMMAND: CLIENT GETNAME":
        try:
            check: waitFor(ar.CLIENT_GETNAME()) == nil
        except UnsupportedError:
            skip()

    test "COMMAND: CLIENT KILL (old version)":
        try:
            check: waitFor(ar.CLIENT_KILL("localhost", 2999'u16)).success == false
        except UnsupportedError:
            skip()

    test "COMMAND: CLIENT KILL (new version)":
        try:
            let filter = newTable({"SKIPME": "yes", "TYPE": "slave"})
            check: waitFor(ar.CLIENT_KILL(filter)) == 0
        except UnsupportedError:
            skip()

    test "COMMAND: CLIENT LIST":
        try:
            check: waitFor(ar.CLIENT_LIST()).len() >= 1
        except UnsupportedError:
            skip()

    test "COMMAND: CLIENT PAUSE":
        try:
            check: waitFor(ar.CLIENT_PAUSE(0)).success
        except UnsupportedError:
            skip()

    test "COMMAND: CLIENT REPLY":
        try:
            check: waitFor(ar.CLIENT_REPLY(ReplyMode.OFF)).success
            check: waitFor(ar.CLIENT_REPLY(ReplyMode.ON)).success
        except UnsupportedError:
            skip()

    test "COMMAND: CLIENT SETNAME":
        try:
            check: waitFor(ar.CLIENT_SETNAME("connection")).success
            check: waitFor(ar.CLIENT_GETNAME()) == "connection"
        except UnsupportedError:
            skip()

    test "COMMAND: COMMAND COUNT":
        try:
            check: waitFor(ar.COMMAND_COUNT()) > 30
        except UnsupportedError:
            skip()

    test "COMMAND: DBSIZE":
        check: waitFor(ar.DBSIZE()) > 0

    test "COMMAND: DEBUG OBJECT":
        let ob = waitFor(ar.DEBUG_OBJECT("hello"))
        check:
            ob.lru > 0
            ob.refcount > 0
            ob.address.startsWith("0x")

    test "COMMAND: DEBUG SEGFAULT (actually not running it for obvious reasons :))":
        skip()

    test "COMMAND: DECR":
        discard waitFor(ar.SET("x", 10))
        check:
            waitFor(ar.DECR("x")).value == 9
            waitFor(ar.DECR("hello")).success == false

    test "COMMAND: DECRBY":
        discard waitFor(ar.SET("x", 10))
        check:
            waitFor(ar.DECRBY("x", 5)).value == 5
            waitFor(ar.DECRBY("hello", 6)).success == false

    test "COMMAND: DEL":
        check:
            waitFor(ar.DEL("non-existing-key")) == 0
            waitFor(ar.DEL(@["nxk1", "nxk2"])) == 0

    test "COMMAND: DISCARD":
        let t = waitFor(ar.transaction())
        check: waitFor(t.DISCARD()).success == false
        t.finish()

    test "COMMAND: DUMP":
        try:
            check: waitFor(ar.DUMP("hello")).len() == 17
        except UnsupportedError:
            skip()

    test "COMMAND: ECHO":
        check: waitFor(ar.ECHO("hello")) == "hello"

    test "COMMAND: FLUSHALL":
        check: waitFor(ar.FLUSHALL()).success

    test "COMMAND: FLUSHDB":
        check: waitFor(ar.FLUSHDB()).success

    test "COMMAND: INFO":
        check: waitFor(ar.INFO()).hasKey("redis_version")

    test "COMMAND: KEYS":
        discard waitFor(ar.SET("keystest_1", "1"))
        discard waitFor(ar.SET("keystest_2", "2"))
        check:
            waitFor(ar.KEYS("keystest_*")).len() == 2

    test "COMMAND: SET":
        check:
            waitFor(ar.SET("string", "world")) == true
            waitFor(ar.SET("int", 5)) == true
            waitFor(ar.SET("float", 5.5)) == true

    test "COMMAND: GET":
        check:
            waitFor(ar.GET("string")) == "world"
            waitFor(ar.GET("int")) == "5"
            waitFor(ar.GET("float")) == "5.5"
            waitFor(ar.GET("non-existing-key")) == nil

    test "COMMAND: INCR":
        discard waitFor(ar.SET("x", 10))
        discard waitFor(ar.SET("hello", "world"))
        check:
            waitFor(ar.INCR("x")).value == 11
            waitFor(ar.INCR("hello")).success == false

    test "COMMAND: INCRBY":
        discard waitFor(ar.SET("x", 10))
        discard waitFor(ar.SET("hello", "world"))
        check:
            waitFor(ar.INCRBY("x", 5)).value == 15
            waitFor(ar.INCRBY("hello", 6)).success == false

    test "COMMAND: INCRBY":
        discard waitFor(ar.SET("x", 10))
        discard waitFor(ar.SET("hello", "world"))
        check:
            waitFor(ar.INCRBYFLOAT("x", 5.5)).value == 15.5
            waitFor(ar.INCRBY("hello", 6)).success == false

    test "COMMAND: LASTSAVE":
        check: timeInfoToTime(waitFor(ar.LASTSAVE())).toSeconds() > 0

    test "COMMAND: LPUSH (old, single param)":
        discard waitFor(ar.SET("slist", "x"))
        check:
            waitFor(ar.LPUSH("nolist", "x")).value == 1
            waitFor(ar.LPUSH("slist", "x")).success == false

    test "COMMAND: LPUSH (new, multi param)":
        discard waitFor(ar.SET("mlist", "x"))
        try:
            check:
                waitFor(ar.LPUSH("nolist2", @["x", "y"])).value == 2
                waitFor(ar.LPUSH("mlist", @["x", "y"])).success == false
        except UnsupportedError:
            skip()

    test "COMMAND: LPUSHX":
        discard waitFor(ar.LPUSH("xlist", "x"))
        try:
            check: 
                waitFor(ar.LPUSHX("xlist", "y")).value == 2
                waitFor(ar.LPUSHX("xnolist", "y")).success == false
        except UnsupportedError:
            skip()

    test "COMMAND: PING":
        check: waitFor(ar.PING())== true

    test "COMMAND: TIME":
        try:
            check: timeInfoToTime(waitFor(ar.TIME())).toSeconds() > 0
        except UnsupportedError:
            skip()

    test "COMMAND: TTL":
        check: waitFor(ar.SET("hello", "world"))
        check: waitFor(ar.TTL("non-existing-key")) == ttlDoesNotExist
        check: waitFor(ar.TTL("hello")) == ttlInfinite

    test "COMMAND: TYPE":
        check:
            waitFor(ar.TYPE("xxxx")) == RedisDataType.None
            waitFor(ar.TYPE("hello")) == RedisDataType.String
