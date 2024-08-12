module Main (main) where

import Net.Redis.Server (server, RedisOriginServer (RedisOriginServer, rosAddress, rosPort))
import Network.Socket (SockAddr(SockAddrInet6))

main :: IO ()
main = server RedisOriginServer {
                rosAddress = SockAddrInet6 16379 0 (0, 0, 0, 0) 0,
                rosPort = 0
              }
