{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Net.Redis.Server 
import Network.Socket (SockAddr(SockAddrInet6))
import Control.Concurrent.STM (TVar)
import Dataflow (Graph, Input, Vertex, using, send, inputVertex)
import Dataflow.Operators (statelessVertex)

graph :: TVar RedisKV -> Graph (Input RedisInputEvent)
graph register = do
  output <- redisOutputVertex register
  inputVertex (yeetTo output)

  where
   yeetTo :: Vertex RedisOutputEvent -> Graph (Vertex RedisInputEvent)
   yeetTo output =
    using output $ \edge ->
      statelessVertex (\ts input ->
          case input of
            RedisSet k v -> send edge ts (RedisScalar ("yeet-" <> k) v)
        )

main :: IO ()
main = do
  server RedisOriginServer {
                rosAddress = SockAddrInet6 16379 0 (0, 0, 0, 0) 0,
                rosPort = 0
              } graph
