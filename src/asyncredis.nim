import asyncdispatch
import asyncnet
import macros
import sequtils
import streams
import strutils
import times

const
    rpNull    = "$-1"  # Redis Protocol NULL string value
    rpOk      = "+OK"  # Redis Protocol `simple string` successfull reply 
    rpInt     = ":"    # Redis Protocol integer reply marker
    rpErr     = "-ERR" # Redis Protocol error reply marker
    rpSuccess = "+"    # Redis Protocol marker for successful command execution 

type
    CommunicationError* = object of Exception
        ## Raises on communication problems with MongoDB server

    AsyncLockedSocket = ref object
        inuse:         bool
        authenticated: bool
        connected:     bool
        sock:          AsyncSocket

    AsyncRedis* = ref object of RootObj
        host:     string
        port:     Port
        
        username: string
        password: string

        current: int
        pool:    seq[AsyncLockedSocket]

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

proc connect*(ar: AsyncRedis): Future[bool] {.async.} =
  ## Establish asynchronous connections with Redis servers
  for ls in ar.pool.items():
    try:
      await ls.sock.connect(ar.host, ar.port)
      ls.connected = true
    except OSError:
      continue
  return any(ar.pool, proc(item: AsyncLockedSocket): bool = item.connected)

# Template for disconnection handling
template handleDisconnect(response: var string, sock: AsyncLockedSocket) =
    if response == "":
        ls.connected = false
        ls.inuse = false
        raise newException(CommunicationError, "Disconnected from MongoDB server")

proc APPEND*(ar: AsyncRedis, key: string, value: string): Future[int64] {.async.} =
    ## `APPEND` string to existing one, if one does not exists - acts like `SET`.
    let
        ls = await ar.next()
        command = "*3\r\n$$6\r\nAPPEND\r\n$$$#\r\n$#\r\n$$$#\r\n$#\r\n".format(key.len(), key, value.len(), value)
    await ls.sock.send(command)

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    return parseInt(data[1 .. ^1])

proc AUTH*(ar: AsyncRedis, password: string): Future[StringStatusReply] {.async.} =
    ## `AUTH`enticates into server. Returns true if successfully authenticated,
    ## and false if not. In that case `lastError` field for the socket is
    ## set to string with error explanation.
    let ls = await ar.next() 
    await ls.sock.send("*2\r\n$$4\r\nAUTH\r\n$$$#\r\n$#\r\n".format(password.len(), password))

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    if data.startsWith(rpErr):
        return (false, data)
    elif data == rpOk:
        return (true, data)

proc BGREWRITEAOF*(ar: AsyncRedis): Future[StringStatusReply] {.async.} =
    ## `BGREWRITEAOF` - rewrite AOF storage file asynchronously
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$12\r\nBGREWRITEAOF\r\n")

    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

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
        return (false, data)
    elif data.startsWith(rpSuccess):
        return (true, data) 

proc BITCOUNT*(ar: AsyncRedis, key: string, indexStart: int = 0, indexEnd: int = -1): Future[int64] {.async.} =
    ## `BITCOUNT` counts number of set bits in value treated as byte stream
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
    let ls = await ar.next()
    await ls.sock.send("*2\r\n$$3\r\nGET\r\n$$$#\r\n$#\r\n".format(key.len(), key))
    
    var data: string = await ls.sock.recvLine()
    handleDisconnect(data, ls)

    if data == rpNull:
        return nil
    else:
        let strlen = parseInt(data[1 .. ^1])
        data = await ls.sock.recv(strlen + 2)
        handleDisconnect(data, ls)

    return data[0 .. ^3]

proc PING*(ar: AsyncRedis): Future[bool] {.async.} =
    ## Send `PING` command in order to receive PONG reply signaling
    ## that everything is okay with database server.
    let ls = await ar.next()
    await ls.sock.send("*1\r\n$4\r\nPING\r\n")
    var data: string = await ls.sock.recv(7)
    handleDisconnect(data, ls)
    return data == "+PONG\r\n"

proc SET*(ar: AsyncRedis, key: string, value: string, ttl: TimeInterval = TimeInterval()): Future[bool] {.async} =
    ## Set value for key
    let ls = await ar.next()
    await ls.sock.send("*3\r\n$3\r\nSET\r\n" & "$" & $key.len() & "\r\n" & key & "\r\n" & "$" & $value.len() & "\r\n" & value & "\r\n")
    var data: string = await ls.sock.recv(5)
    handleDisconnect(data, ls)
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