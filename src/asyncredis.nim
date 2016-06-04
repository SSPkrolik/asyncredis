import asyncdispatch
import asyncnet
import macros
import sequtils
import streams
import strutils
import tables
import times

const
    rpNull    = "$-1"   # Redis Protocol NULL string value
    rpOk      = "+OK"   # Redis Protocol `simple string` successfull reply
    rpInt     = ":"     # Redis Protocol integer reply marker
    rpErr     = "-ERR"  # Redis Protocol error reply marker
    rpSuccess = "+"     # Redis Protocol marker for successful command execution
    rpBulk    = "*"     # Redis Protocol marker for bulk string reply
    rpNewLine = "\r\n"  # Redis Protocol new-line marker

    ttlDoesNotExist* = -2  ## TTL for non-existing key
    ttlInfinite*     = -1  ## TTL for key without expiration time

    reString*  = "embstr"  ## Redis encoding for strings

type
    Architecture* {.pure.} = enum
        ## Returned within INFO command reply
        X86    = 0
        X86_64 = 1

    BitOperation* {.pure.} = enum
        ## Used by BITOP command
        AND
        OR
        XOR
        NOT

    ReplyMode* {.pure.} = enum
        ## Can be set using CLIENT REPLY command
        ON    ## Server always replies
        OFF   ## Server do not reply
        SKIP  ## Server do not reply immediately

    ObjectDebugInfo* = object
        address*:        string
        refcount*:       int
        encoding*:       string
        serializedLen*:  int
        lru*:            int
        lruSecondsIdle*: int

    CommunicationError* = object of Exception
        ## Raises on communication problems with MongoDB server

    UnsupportedError* = object of Exception
        ## Raises when trying to send command unsupported by
        ## connected version of server

    AsyncLockedSocket = ref object
        inuse:         bool
        authenticated: bool
        connected:     bool
        sock:          AsyncSocket

    RedisVersion* = tuple[major: int, minor: int, micro: int]
        ## Version of Redis server returned within INFO command reply

    RedisDataType* {.pure.} = enum
        None
        String
        List
        Set
        ZSet
        Hash

    AsyncRedis* = ref object of RootObj
        ## Asynchronous Redis client
        host:     string
        port:     Port

        username: string
        password: string

        current: int
        pool:    seq[AsyncLockedSocket]

        replyMode: ReplyMode

        infocached: bool
        version:    RedisVersion

    Transaction* = ref object
        ## Transaction is use to perform EXEC/MULTI/WATCH/UNWATCH
        ## commands in one dedicated connection
        ls:   AsyncLockedSocket
        pool: AsyncRedis

    StringStatusReply* = tuple[success: bool, message: string]
        ## This reply consists of first field indicating success or failure
        ## of the command execution, and the second identifies the reason
        ## of failure or description of success received from Redis server.

    IntegerStatusReply* = tuple[success: bool, value: int64, message: string]
        ## This reply consists of first field indicating success or failure
        ## of the command execution, the second contains response value in
        ## case of execution success, and the third equals to error message
        ## when execution error happened.

    FloatStatusReply* = tuple[success: bool, value: float64, message: string]
        ## This reply consists of first field indicating success of failure
        ## of the command execution, the second contains response value in
        ## case of execution success, and the third equals to error message
        ## when execution error happened.

proc toRedisType(s: string): RedisDataType =
    case s
    of "string":
        return RedisDataType.String
    of "list":
        return RedisDataType.List
    of "set":
        return RedisDataType.Set
    of "zset":
        return RedisDataType.ZSet
    of "hash":
        return RedisDataType.Hash
    else:
        return RedisDataType.None

proc `<`*(v1, v2: RedisVersion): bool =
    if v1.major < v2.major:
        return true
    elif v1.major > v2.major:
        return false
    else:
        if v1.minor < v2.minor:
            return true
        elif v1.minor > v2.minor:
            return false
        else:
            if v1.micro < v2.micro:
                return true
            else:
                return false    

proc `==`*(v1, v2: RedisVersion): bool =
    v1.major == v2.major and v1.minor == v2.minor and v1.micro == v2.micro

template `>`*(v1, v2: RedisVersion): bool =
    (not (v1 < v2)) and (not (v1 == v2))

proc newAsyncLockedSocket(): AsyncLockedSocket =
  ## Constructor for "locked" async socket
  return AsyncLockedSocket(
    inuse:         false,
    authenticated: false,
    connected:     false,
    sock:          newAsyncSocket()
  )

proc returnSocket(ar: AsyncRedis, ls: AsyncLockedSocket) =
    ar.pool.add(ls) 

proc next(ar: AsyncRedis, borrowSocket: bool = false): Future[AsyncLockedSocket] {.async.} =
  ## Retrieves next non-in-use async socket for request
  while true:
    for _ in 0 ..< ar.pool.len():
      ar.current = (ar.current + 1) mod ar.pool.len()
      if not ar.pool[ar.current].inuse:
        if not ar.pool[ar.current].connected:
          try:
            await ar.pool[ar.current].sock.connect(ar.host, ar.port)
            ar.pool[ar.current].connected = true
          except OSError:
            continue
        ar.pool[ar.current].inuse = true
        let sockToReturn = ar.pool[ar.current]
        if borrowSocket:
            ar.pool.delete(ar.current, ar.current)
        return sockToReturn
    await sleepAsync(1)

template since(v: RedisVersion): untyped {.immediate.} =
    ## Checks minimum required version for commmand and raises error
    ## if version is unsupported.
    if ar.version < v:
        raise newException(UnsupportedError, "This command is unsupported for this version of Redis")

proc newAsyncRedis*(host: string, port: Port = Port(6379), username: string = nil, password: string = nil, poolSize=16): AsyncRedis =
    ## Constructor for Redis async client
    result.new()
    result.current = -1

    result.host = host
    result.port = port

    result.username = username
    result.password = password

    result.replyMode = ReplyMode.ON

    result.pool = @[]
    for i in 0 ..< poolSize:
        result.pool.add(newAsyncLockedSocket())

proc version*(ar: AsyncRedis): RedisVersion = return ar.version
    ## Return full version of Redis server

template handleDisconnect(response: var string, ls: AsyncLockedSocket) =
    ## Template for disconnection handling
    if response == "":
        ls.connected = false
        ls.inuse = false
        raise newException(CommunicationError, "Disconnected from MongoDB server")

proc INFO*(ar: AsyncRedis, refresh: bool = false): Future[TableRef[string, string]] {.async.} =
    ## Send `INFO` command to server retrieving server-related
    ## information. If `refresh` parameter is set to true, Redis client
    ## info is cached from response.
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$4\r\nINFO\r\n")

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    data = await ls.sock.recv(parseInt(data[1 .. ^1]) + 2)
    handleDisconnect(data, ls)

    ls.inuse = false

    result = newTable[string, string]()

    let dataStream = newStringStream(data)
    while not dataStream.atEnd():
        let line = dataStream.readLine().string
        if line.startsWith("#"):
            continue
        else:
            let parts = line.split(":")
            if len(parts) == 2:
                result[parts[0]] = parts[1]
            elif len(parts) == 1:
                result[parts[0]] = ""
            elif len(parts) == 0:
                continue

            if refresh:
                case parts[0]
                of "redis_version":
                    let vparts = map(parts[1].split("."), parseInt)
                    ar.version = (vparts[0], vparts[1], vparts[2])
                    # TODO: Add more values to cache
                else:
                    discard

proc connect*(ar: AsyncRedis): Future[bool] {.async.} =
  ## Establish asynchronous connections with Redis servers
  for ls in ar.pool.items():
    try:
      await ls.sock.connect(ar.host, ar.port)
      ls.connected = true
    except OSError:
      continue
    if not ar.infocached:
      try:
        discard await ar.INFO(refresh = true)
        ar.infocached = true
      except CommunicationError:
        continue
  return any(ar.pool, proc(item: AsyncLockedSocket): bool = item.connected)

proc APPEND*(ar: AsyncRedis, key: string, value: string): Future[int64] {.async.} =
    ## `APPEND` string to existing one, if one does not exists - acts like `SET`.
    since ((2, 0, 0))
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nAPPEND\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, value.len(), value)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

proc AUTH*(ar: AsyncRedis, password: string): Future[StringStatusReply] {.async.} =
    ## `AUTH`enticates into server. Returns true if successfully authenticated,
    ## and false if not. In that case `lastError` field for the socket is
    ## set to string with error explanation.
    let ls = await ar.next()
    await ls.sock.send("*2\r\n$$4\r\nAUTH\r\n$$$#\r\n$#\r\n".format(password.len(), password))

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return (data == rpOk, data)

proc BGREWRITEAOF*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## `BGREWRITEAOF` - rewrite AOF storage file asynchronously
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$12\r\nBGREWRITEAOF\r\n")

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    if data.startsWith(rpErr):
        return (false, data)
    elif data.startsWith(rpSuccess):
        return (true, data)

proc BGSAVE*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## `BGSAVE` - Save DB in background
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$6\r\nBGSAVE\r\n")

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    if data.startsWith(rpErr):
        ls.inuse = false
        return (false, data)
    elif data.startsWith(rpSuccess):
        ls.inuse = false
        return (true, data)

proc BITCOUNT*(ar: AsyncRedis, key: string, indexStart: int = 0, indexEnd: int = -1): Future[int64] {.async.} =
    ## `BITCOUNT` counts number of set bits in value treated as byte stream
    since ((2, 6, 0))
    let ls = await ar.next()
    let command = if indexStart == 0 and indexEnd == -1:
                      "*2\r\n$$8\r\nBITCOUNT\r\n$$$#\r\n$#\r\n".format(key.len(), key)
                  else:
                      "*4\r\n$$8\r\nBITCOUNT\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(
                          key.len(), key, ($indexStart).len(), indexStart, ($indexEnd).len(), indexEnd
                      )
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

# proc BITFIELD

proc BITOP*(ar: AsyncRedis, operation: BitOperation, dest: string, keys: seq[string]): Future[int64] {.async.} =
    ## Perform bitwise operation between `keys` and write result into `destination` key
    since((2, 6, 0))
    if operation == BitOperation.Not:
        doAssert(keys.len() == 1)

    let ls = await ar.next()
    var command: string = "*$#\r\n$$5\r\nBITOP\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(keys.len() + 3, ($operation).len(), $operation, dest.len(), dest)
    for key in keys: command &= "$$$#\r\n$#\r\n".format(key.len(), key)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

# proc BITPOS
# proc BLPOP
# proc BRPOP
# proc BRPOPLPUSH

proc CLIENT_GETNAME*(ar: AsyncRedis): Future[string] {.async.} = 
    ## `CLIENT GETNAME` returns client's connection name which can
    ## be set using `CLIENT SETNAME` command. If no name was
    ## set, nil is returned.
    since((2, 6, 9))
    let
        ls = await ar.next()
        command = "*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n"
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    if data == rpNull:
        ls.inuse = false
        return nil

    data = await ls.sock.recv(parseInt(data[1 .. ^1]) + 2)
    handleDisconnect(data, ls)

    ls.inuse = false
    result = data[0 .. ^3]

proc CLIENT_KILL*(ar: AsyncRedis, address: string, port: uint16): Future[StringStatusReply] {.async.} =
    ## Old method (prior to Redis 2.8.11) to disconnect clients from server
    since((2, 4, 0))
    let
        ls = await ar.next()
        target = "$#:$#".format(address, port)
        command = "*3\r\n$$6\r\nCLIENT\r\n$$4\r\nKILL\r\n$$$#\r\n$#\r\n".format(target.len(), target)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return (data == rpOk, data)

proc CLIENT_KILL*(ar: AsyncRedis, filters: TableRef[string, string]): Future[int64] {.async.} =
    ## New method (since Redis 2.8.11) to disconnect clients from server
    since((2, 8 , 11))
    let ls = await ar.next()
    var command = "*$#\r\n$$6\r\nCLIENT\r\n$$4\r\nKILL\r\n".format(filters.len() * 2 + 2)
    for name, val in filters:
        command &= "$$$#\r\n$#\r\n$$$#\r\n$#\r\b".format(name.len(), name, val.len(), val)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

proc CLIENT_LIST*(ar: AsyncRedis): Future[seq[string]] {.async.} =
    ## `CLIENT LIST` returns list of clients connected to server.
    since((2, 4, 0))
    let
        ls = await ar.next()
        command = "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n"
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    data = await ls.sock.recv(parseInt(data[1 .. ^1]) + 2)
    handleDisconnect(data, ls)

    ls.inuse = false

    result = @[]

    let dataStream = newStringStream(data)
    while not dataStream.atEnd():
        let line = dataStream.readLine().string
        result.add(line)

proc CLIENT_PAUSE*(ar: AsyncRedis, timeout: uint): Future[StringStatusReply] {.async.} =
    ## Pauses all clients for certain amout of time specified in seconds
    since((2, 9, 50))
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nCLIENT\r\n$$5\r\nPAUSE\r\n$$$#\r\n$#\r\n".format(($timeout).len(), $timeout)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return (data == rpOk, data)

proc CLIENT_REPLY*(ar: AsyncRedis, mode: ReplyMode): Future[StringStatusReply] {.async.} = 
    ## Sets reply mode for server and current client
    since((3, 2, 0))
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nCLIENT\r\n$$5\r\nREPLY\r\n$$$#\r\n$#\r\n".format(($mode).len(), $mode)
    await ls.sock.send(command)

    if mode == ReplyMode.ON:
        var data: string = await ls.sock.recvLine()
        handleDisconnect(data, ls)
        ls.inuse = false
        return (data == rpOk, data)
    else:
        ls.inuse = false
        return (true, nil)

proc CLIENT_SETNAME*(ar: AsyncRedis, name: string): Future[StringStatusReply] {.async.} =
    ## Sets current connection name which can be retrieved using GETNAME
    since((2, 6, 9))
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nCLIENT\r\n$$7\r\nSETNAME\r\n$$$#\r\n$#\r\n".format(name.len(), name)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return (data == rpOk, data)

# CLUSTER ADDSLOTS
# CLUSTER COUNT-FAILURE-REPORTS
# CLUSTER COUNTKEYSINSLOT
# CLUSTER DELSLOTS
# CLUSTER FAILOVER
# CLUSTER FORGET
# CLUSTER GETKEYSINSLOT
# CLUSTER INFO
# CLUSTER KEYSLOT
# CLUSTER MEET
# CLUSTER NODES
# CLUSTER REPLICATE
# CLUSTER RESET
# CLUSTER SAVECONFIG
# CLUSTER SET-CONFIG-EPOCH
# ClUSTER SETSLOT
# CLUSTER SLAVES
# CLUSTER SLOTS

# COMMAND

proc COMMAND_COUNT*(ar: AsyncRedis): Future[int64] {.async.} =
    ## Returns number of supported commands by connected Redis server
    since((2, 8, 13))
    let
        ls = await ar.next()
        command = "*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n"
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

# COMMAND GETKEYS
# COMMAND INFO

# CONFIG GET
# CONFIG REWRITE
# CONFIG SET
# CONFIG RESETSTAT

proc DBSIZE*(ar: AsyncRedis): Future[int64] {.async.} =
    ## Returns number of keys inside currently selected database
    let
        ls = await ar.next()
        command = "*1\r\n$6\r\nDBSIZE\r\n"
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

proc DEBUG_OBJECT*(ar: AsyncRedis, key: string): Future[ObjectDebugInfo] {.async.} =
    ## Returns inner Redis debug info for object
    let
        ls = await ar.next()
        command = "*3\r\n$$5\r\nDEBUG\r\n$$6\r\nOBJECT\r\n$$$#\r\n$#\r\n".format(key.len(), key)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    let lines = data[0 .. ^3].split(" ")[1 .. ^1]
    result = ObjectDebugInfo()
    for line in lines:
        let pair = line.split(":")
        case pair[0]
        of "at":
            result.address = pair[1]
        of "refcount":
            result.refcount = parseInt(pair[1])
        of "encoding":
            result.encoding = pair[1]
        of "serializedlength":
            result.serializedLen = parseInt(pair[1])
        of "lru":
            result.lru = parseInt(pair[1])
        else:
            discard

proc DEBUG_SEGFAULT*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## Makes Redis server to crash
    let
        ls = await ar.next()
        command = "*2\r\n$5\r\nDEBUG\r\n$8\r\nSEGFAULT\r\n"
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return (data == rpOk, data)

proc DECR*(ar: AsyncRedis, key: string): Future[IntegerStatusReply] {.async.} =
    ## Decrease value stored by the key by 1.
    let
        ls = await ar.next()
        command = "*2\r\n$$4\r\nDECR\r\n$$$#\r\n$#\r\n".format(key.len(), key)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    if data.startsWith(rpInt):
        return (true, parseInt(data[1 .. ^1]).int64, nil.string)
    else:
        return (false, 0.int64, data)

proc DECRBY*(ar: AsyncRedis, key: string, by: int64): Future[IntegerStatusReply] {.async.} =
    ## Decrease value stored by the key by 1.
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nDECRBY\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, ($by).len(), by)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    if data.startsWith(rpInt):
        result = (true, parseInt(data[1 .. ^1]).int64, nil.string)
    else:
        result = (false, 0.int64, data)

proc DEL*(ar: AsyncRedis, keys: seq[string]): Future[int64] {.async.} =
    ## Remove specified keys from database
    let ls = await ar.next()
    var command: string = "*$#\r\n$$3\r\nDEL\r\n".format(keys.len() + 1)
    for key in keys: command &= "$$$#\r\n$#\r\n".format(key.len(), key)

    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

proc DEL*(ar: AsyncRedis, key: string): Future[int64] {.async.} =
    ## Delete single key from database
    result = await ar.DEL(@[key])

proc DISCARD*(t: Transaction): Future[StringStatusReply] {.async.} =
    ## Discard unwatches all keys for current connection after using
    ## WATCH command
    let command = "*1\r\n$7\r\nDISCARD\r\n"
    await t.ls.sock.send(command)

    var data: string = await t.ls.sock.recvLine()
    handleDisconnect(data, t.ls)

    return (data == rpOk, data)

proc DUMP*(ar: AsyncRedis, key: string): Future[string] {.async.} =
    ## Return value stored in key in Redis-specific serialized format
    since((2, 6, 0))
    let ls = await ar.next()
    await ls.sock.send("*2\r\n$$4\r\nDUMP\r\n$$$#\r\n$#\r\n".format(key.len(), key))

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    let strlen = parseInt(data[1 .. ^1])
    data = await ls.sock.recv(strlen + 2)
    handleDisconnect(data, ls)

    ls.inuse = false
    return data[0 .. ^3]


proc ECHO*(ar: AsyncRedis, message: string): Future[string] {.async.} =
    ## Push message to server and receive it from it, like ping but
    ## configurable
    let
        ls = await ar.next()
        command = "*2\r\n$$4\r\nECHO\r\n$$$#\r\n$#\r\n".format(message.len(), message)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    let strlen = parseInt(data[1 .. ^1])
    data = await ls.sock.recv(strlen + 2)
    handleDisconnect(data, ls)

    ls.inuse = false
    return data[0 .. ^3]

# EVAL
# EVALSHA
# EXEC
# EXISTS
# EXPIRE
# EXPIREAT

proc FLUSHALL*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## Removes all keys from server databases
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$8\r\nFLUSHALL\r\n")

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return (data == rpOk, data)

proc FLUSHDB*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## Removes all keys from currently selected database
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$7\r\nFLUSHDB\r\n")

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return (data == rpOk, data)

# GEOADD
# GEOHASH
# GEOPOS
# GEODIST
# GEORADIUS
# GEORADIUSBYMEMBER

proc GET*(ar: AsyncRedis, key: string): Future[string] {.async.} =
    ## `GET` value from database by key
    let
        ls = await ar.next()
        command = "*2\r\n$$3\r\nGET\r\n$$$#\r\n$#\r\n".format(key.len(), key)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    if data == rpNull or data == rpNewLine:
        ls.inuse = false
        return nil
    else:
        let strlen = parseInt(data[1 .. ^1])
        data = await ls.sock.recv(strlen + 2)
        handleDisconnect(data, ls)

    ls.inuse = false
    return data[0 .. ^3]

# GETBIT
# GETRANGE
# GETSET

# HDEL
# HEXISTS
# HGET
# HGETALL
# HINCRBY
# HINCRBYFLOAT
# HKEYS
# HLEN
# HMGET
# HMSET
# HSET
# HSETNX
# HSTRLEN
# HVALS

proc INCR*(ar: AsyncRedis, key: string): Future[IntegerStatusReply] {.async.} =
    ## Increase value stored by the key by 1.
    let
        ls = await ar.next()
        command = "*2\r\n$$4\r\nINCR\r\n$$$#\r\n$#\r\n".format(key.len(), key)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    if data.startsWith(rpInt):
        result = (true, parseInt(data[1 .. ^1]).int64, nil.string)
    else:
        result = (false, 0.int64, data[1 .. ^1])

proc INCRBY*(ar: AsyncRedis, key: string, by: int64): Future[IntegerStatusReply] {.async.} =
    ## Decrease value stored by the key by 1.
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nINCRBY\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, ($by).len(), by)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    if data.startsWith(rpInt):
        result = (true, parseInt(data[1 .. ^1]).int64, nil.string)
    else:
        result = (false, 0.int64, data[1 .. ^1])

proc INCRBYFLOAT*(ar: AsyncRedis, key: string, by: float64): Future[FloatStatusReply] {.async.} =
    ## Increase value stored by the key by floating-point number
    since((2, 6, 0))
    let
        ls = await ar.next()
        command = "*3\r\n$$11\r\nINCRBYFLOAT\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, ($by).len(), by)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    if data.startsWith(rpErr):
        ls.inuse = false
        return (false, 0.0'f64, data[1 .. ^1])
    else:
        let strlen = parseInt(data[1 .. ^1])

        data = await ls.sock.recv(strlen + 2)
        handleDisconnect(data, ls)

        ls.inuse = false

        return (true, parseFloat(data[0 .. ^3]), nil.string)

proc KEYS*(ar: AsyncRedis, pattern: string): Future[seq[string]] {.async.} =
    ## Return all keys from database matching the pattern.
    result = @[]
    let
        ls = await ar.next()
        command = "*2\r\n$$4\r\nKEYS\r\n$$$#\r\n$#\r\n".format(pattern.len(), pattern)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    let seqlen = parseInt(data[1 .. ^1])

    if seqlen == 0:
        ls.inuse = false
    else:
        for i in 0 .. <seqlen:
            data = await ls.sock.recvLine()
            handleDisconnect(data, ls)

            let strlen = parseInt(data[1 .. ^1])
            data = await ls.sock.recv(strlen + 2)
            handleDisconnect(data, ls)
            result.add(data[0 .. ^3])
        ls.inuse = false

proc LASTSAVE*(ar: AsyncRedis): Future[TimeInfo] {.async.} =
    ## Return time of last save performed by Redis server
    let
        ls = await ar.next()
        command = "*1\r\n$8\r\nLASTSAVE\r\n"
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return timeToTimeInfo(fromSeconds(parseInt(data[1 .. ^1])))

# LINDEX
# LINSERT
# LLEN
# LPOP
# LPUSH

proc LPUSH*(ar: AsyncRedis, key: string, value: string): Future[IntegerStatusReply] {.async.} =
    ## Pushes new value at the head of the list stored by key
    let
        ls = await ar.next()
        command = "*3\r\n$$5\r\nLPUSH\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, value.len(), value)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    if data.startsWith(rpInt):
        return (true, parseInt(data[1 .. ^1]).int64, nil.string)
    else:
        return (false, 0'i64, data[1 .. ^1])

proc LPUSH*(ar: AsyncRedis, key: string, values: seq[string]): Future[IntegerStatusReply] {.async.} =
    ## Pushes new values at the head of the list stored by key
    since((2, 4, 0))
    let ls = await ar.next()
    var command = "*$#\r\n$$5\r\nLPUSH\r\n$$$#\r\n$#\r\n".format(2 + values.len(), key.len(), key)
    for val in values: command &= "$$$#\r\n$#\r\n".format(val.len(), val)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    if data.startsWith(rpInt):
        return (true, parseInt(data[1 .. ^1]).int64, nil.string)
    else:
        return (false, 0'i64, data[1 .. ^1])

proc LPUSHX*(ar: AsyncRedis, key: string, value: string): Future[IntegerStatusReply] {.async.} =
    ## Pushes value at the head of the list only if the key exists and
    ## already holds a list
    since((2, 2, 0))
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nLPUSHX\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, value.len(), value)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    if data.startsWith(rpInt):
        let val = parseInt(data[1 .. ^1]).int64
        if val == 0:
            return (false, val , nil.string)
        return (true, val, nil.string)
    else:
        return (false, 0'i64, data[1 .. ^1])

# LRANGE
# LREM
# LSET
# LTRIM

# MGET

# MIGRATE
# MONITOR
# MOVE
# MSET
# MSETNX
# MULTI

# OBJECT
# PERSIST
# PEXPIRE
# PEXPIREAT

# PFADD
# PFCOUNT
# PFMERGE

proc PING*(ar: AsyncRedis): Future[bool] {.async.} =
    ## Send `PING` command in order to receive PONG reply signaling
    ## that everything is okay with database server.
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$4\r\nPING\r\n")
    var data: string = await ls.sock.recv(7)
    handleDisconnect(data, ls)
    ls.inuse = false
    return data == "+PONG\r\n"

# PSETEX
# PSUBSCRIBE
# PUBSUB
# PTTL
# PUBLISH
# PUNSUBSCRIBE

# QUIT

# RANDOMKEY
# READONLY
# READWRITE
# RENAME
# RENAMENX
# RESTORE
# ROLE

# RPOP
# RPOPLPUSH
# RPUSH
# RPUSHX

# SADD
# SAVE
# SCARD
# SCRIPT DEBUG
# SCRIPT EXISTS
# SCRIPT FLUSH
# SCRIPT KILL
# SCRIPT LOAD
# SDIFF
# SDIFFSTORE
# SELECT

proc SET*(ar: AsyncRedis, key: string, value: string, ttl: TimeInterval = TimeInterval()): Future[bool] {.async} =
    ## Set value for key
    let ls = await ar.next()
    await ls.sock.send("*3\r\n$3\r\nSET\r\n" & "$" & $key.len() & "\r\n" & key & "\r\n" & "$" & $value.len() & "\r\n" & value & "\r\n")
    var data: string = await ls.sock.recv(5)
    handleDisconnect(data, ls)
    ls.inuse = false
    return data == "+OK\r\n"

proc SET*(ar: AsyncRedis, key: string, value: int64, ttl: TimeInterval = TimeInterval()): Future[bool] {.async.} =
    ## Set integer value for key. Though in Redis everything is considered
    ## as a string, Redis have some commands to process numbers.
    let res = await ar.SET(key, $value, ttl)
    return res

proc SET*(ar: AsyncRedis, key: string, value: float64, ttl: TimeInterval = TimeInterval()): Future[bool] {.async.} =
    ## Set integer value for key. Though in Redis everything is considered
    ## as a string, Redis have some commands to process numbers.
    let res = await ar.SET(key, $value, ttl)
    return res

# SETBIT
# SETEX
# SETNX
# SETRANGE
# SHUTDOWN
# SINTER
# SINTERSTORE
# SISMEMBER
# SLAVEOF
# SLOWLOG
# SMEMBERS
# SMOVE
# SORT
# SPOP
# SRANDMEMBER
# SREM
# STRLEN
# SUBSCRIBE
# SUNION
# SUNIONSTORE

# SYNC

proc TIME*(ar: AsyncRedis): Future[TimeInfo] {.async.} =
    ## Returns server time
    since((2, 6, 0))
    let
        ls = await ar.next()
        command = "*1\r\n$4\r\nTIME\r\n"
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    doAssert(parseInt(data[1 .. ^1]) == 2)

    data = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    data = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    let seconds = parseInt(data[1 .. ^1])

    data = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    data = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false

    let microseconds = parseInt(data[1 .. ^1])
    result = timeToTimeInfo(fromSeconds(seconds))

proc TTL*(ar: AsyncRedis, key: string): Future[int64] {.async.} =
    ## Returns time-to-live for key:
    ## -2 for non-existing key
    ## -1 for key without expiration
    ## n seconds remaining for key to exit
    let
        ls = await ar.next()
        command = "*2\r\n$$3\r\nTTL\r\n$$$#\r\n$#\r\n".format(key.len(), key)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return parseInt(data[1 .. ^1])

proc TYPE*(ar: AsyncRedis, key: string): Future[RedisDataType] {.async.} =
    ## Return type of value stored by the key
    let
        ls = await ar.next()
        command = "*2\r\n$$4\r\nTYPE\r\n$$$#\r\n$#\r\n".format(key.len(), key)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    ls.inuse = false
    return toRedisType(data[1 .. ^1])

# UNSUBSCRIBE
# UNWATCH
# WAIT
# WATCH

# ZADD
# ZCARD
# ZCOUNT
# ZINCRBY
# ZINTERSTORE
# ZLEXCOUNT
# ZRANGE
# ZRANGEBYLEX
# ZREVRANGEBYLEX
# ZRANGEBYSCORE
# ZRANK
# ZREM
# ZREMRANGEBYLEX
# ZREMRANGEBYRANK
# ZREMRANGEBYSCORE
# ZREVRANGE
# ZREVRANGEBYSCORE
# ZREVRANK
# ZSCORE
# ZUNIONSTORE

# SCAN
# SSCAN
# HSCAN
# ZSCAN

proc transaction*(ar: AsyncRedis): Future[Transaction] {.async.} =
    ## Create new transaction which dedicates locked socket to it
    result.new()
    result.ls = await ar.next(borrowSocket = true)
    result.ls.inuse = true
    result.pool = ar

proc finish*(t: Transaction) =
    ## Finish transaction and return socket to pool
    t.ls.inuse = false
    t.pool.returnSocket(t.ls)

