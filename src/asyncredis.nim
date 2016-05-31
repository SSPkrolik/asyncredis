import asyncdispatch
import asyncnet
import macros
import sequtils
import streams
import strutils
import tables
import times

const
    rpNull    = "$-1"  # Redis Protocol NULL string value
    rpOk      = "+OK"  # Redis Protocol `simple string` successfull reply
    rpInt     = ":"    # Redis Protocol integer reply marker
    rpErr     = "-ERR" # Redis Protocol error reply marker
    rpSuccess = "+"    # Redis Protocol marker for successful command execution
    rpBulk    = "*"    # Redis Protocol marker for bulk string reply
    rpNewLine = "\r\n"

type
    Architecture* {.pure.} = enum
        X86
        X86_64

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

    AsyncRedis* = ref object of RootObj
        host:     string
        port:     Port

        username: string
        password: string

        current: int
        pool:    seq[AsyncLockedSocket]

        version: RedisVersion


    StringStatusReply* = tuple[success: bool, message: string]
        ## This reply consists of first field indication success or failure
        ## of the command execution, and the second identifies the reason
        ## of failure or description of success received from Redis server.

proc newAsyncLockedSocket(): AsyncLockedSocket =
  ## Constructor for "locked" async socket
  return AsyncLockedSocket(
    inuse:         false,
    authenticated: false,
    connected:     false,
    sock:          newAsyncSocket()
  )

proc next(ar: AsyncRedis): Future[AsyncLockedSocket] {.async.} =
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
        return ar.pool[ar.current]
    await sleepAsync(1)

template since(v: RedisVersion, body: stmt): untyped {.immediate.} =
    ## Checks minimum required version for commmand and raises error
    ## if version is unsupported.
    if ar.version.major < v[0]:
        raise newException(UnsupportedError, "This command is unsupported for this version of Redis")
    elif ar.version.major >= v[0]:
        if ar.version.minor < v[1]:
            raise newException(UnsupportedError, "This command is unsupported for this version of Redis")
    body

proc newAsyncRedis*(host: string, port: Port = Port(6379), username: string = nil, password: string = nil, poolSize=16): AsyncRedis =
    ## Constructor for Redis async client
    result.new()
    result.current = -1

    result.host = host
    result.port = port

    result.username = username
    result.password = password

    result.pool = @[]
    for i in 0 ..< poolSize:
        result.pool.add(newAsyncLockedSocket())

proc version*(ar: AsyncRedis): RedisVersion = return ar.version
    ## Return full version of Redis server

template handleDisconnect(response: var string, sock: AsyncLockedSocket) =
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
    ls.inuse = true
    await ls.sock.send("*1\r\n$4\r\nINFO\r\n")

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    data = await ls.sock.recv(parseInt(data[1 .. ^1]))
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
  var infocached = false
  for ls in ar.pool.items():
    try:
      await ls.sock.connect(ar.host, ar.port)
      ls.connected = true
      if not infocached:
        discard ar.INFO(refresh = true)
        infocached = true
    except OSError:
      continue
  return any(ar.pool, proc(item: AsyncLockedSocket): bool = item.connected)

proc APPEND*(ar: AsyncRedis, key: string, value: string): Future[int64] {.async.} =
    ## `APPEND` string to existing one, if one does not exists - acts like `SET`.
    since ((2, 0, 0)):
        let
            ls = await ar.next()
            command = "*3\r\n$$6\r\nAPPEND\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, value.len(), value)
        ls.inuse = true
        await ls.sock.send(command)

        var data: string = await ls.sock.recvLine()
        handleDisconnect(data, ls)

        ls.inuse = false
        return parseInt(data[1 .. ^1])

proc AUTH*(ar: AsyncRedis, password: string): Future[StringStatusReply] {.async.} =
    ## `AUTH`enticates into server. Returns true if successfully authenticated,
    ## and false if not. In that case `lastError` field for the socket is
    ## set to string with error explanation.
    since ((1, 0, 0)):
        let ls = await ar.next()
        ls.inuse = true
        await ls.sock.send("*2\r\n$$4\r\nAUTH\r\n$$$#\r\n$#\r\n".format(password.len(), password))

        var data: string = await ls.sock.recvLine()
        handleDisconnect(data, ls)

        if data.startsWith(rpErr):
            ls.inuse = false
            return (false, data)
        elif data == rpOk:
            ls.inuse = false
            return (true, data)

proc BGREWRITEAOF*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## `BGREWRITEAOF` - rewrite AOF storage file asynchronously
    since ((1, 0, 0)):
        let ls = await ar.next()
        ls.inuse = true
        await ls.sock.send("*1\r\n$12\r\nBGREWRITEAOF\r\n")

        var data: string = await ls.sock.recvLine()
        handleDisconnect(data, ls)

        if data.startsWith(rpErr):
            ls.inuse = false
            return (false, data)
        elif data.startsWith(rpSuccess):
            ls.inuse = false
            return (true, data)

proc BGSAVE*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## `BGSAVE` - Save DB in background
    since ((1, 0, 0)):
        let ls = await ar.next()
        ls.inuse = true
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
    since ((2, 6, 0)):
        let ls = await ar.next()
        ls.inuse = true
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
# proc BITOP
# proc BITPOS
# proc BLPOP
# proc BRPOP
# proc BRPOPLPUSH
# proc CLIENT_KILL

proc CLIENT_LIST*(ar: AsyncRedis): Future[seq[string]] {.async.} =
    ## `CLIENT LIST` returns list of clients connected to server.

proc GET*(ar: AsyncRedis, key: string): Future[string] {.async.} =
    ## `GET` value from database by key
    since ((1, 0, 0)):
        let
            ls = await ar.next()
            command = "*2\r\n$$3\r\nGET\r\n$$$#\r\n$#\r\n".format(key.len(), key)
        ls.inuse = true
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

proc PING*(ar: AsyncRedis): Future[bool] {.async.} =
    ## Send `PING` command in order to receive PONG reply signaling
    ## that everything is okay with database server.
    since ((1, 0, 0)):
        let ls = await ar.next()
        ls.inuse = true
        await ls.sock.send("*1\r\n$4\r\nPING\r\n")
        var data: string = await ls.sock.recv(7)
        handleDisconnect(data, ls)
        ls.inuse = false
        return data == "+PONG\r\n"

proc SET*(ar: AsyncRedis, key: string, value: string, ttl: TimeInterval = TimeInterval()): Future[bool] {.async} =
    ## Set value for key
    since ((1, 0, 0)):
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

#[
proc buildInfixLine(args: seq[NimNode]): NimNode {.compileTime.} =
    if len(args) == 2:
        return infix(args[0], "&", args[1])
    else:
        return infix(args[0], "&", buildInfixLine(args[1 .. ^1]))

proc buildRedisCommand(args: seq[NimNode]): expr {.compileTime.} =
    ## Build Redis Command from Parameters
    var numCmd = args.len().int
    let cmd: string = "*" & $numCmd & "\r\n" & "$" & $($numCmd).len()
    var newargs: seq[NimNode] = @[newLit(cmd)] & args
    return buildInfixLine(newargs)

macro redisCommand(args: varargs[expr]): expr =
    #echo treeRepr(buildRedisCommand(args))
    var newargs: seq[NimNode] = @[]
    for arg in args:
        newargs.add(arg.NimNode)
    let built = buildRedisCommand(newargs)
    echo treeRepr(built)
    return built
]#
