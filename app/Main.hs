{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Control.Concurrent.STM (TVar)
import qualified Data.Map               as Map
import           Dataflow               (Graph, Input, Vertex, inputVertex,
                                         send, using)
import           Dataflow.Operators     (statelessVertex)
import           Net.Redis.Server
import           Network.Socket         (SockAddr (SockAddrInet6))

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
            RedisSet k v -> do
              send edge ts (RedisScalar ("yeet-" <> k) v)
              send edge ts (RedisHashUpdate "yeets" $ Map.insert k ("beyoten-" <> v))
      )

main :: IO ()
main = do
  server RedisOriginServer {
                rosAddress = SockAddrInet6 16379 0 (0, 0, 0, 0) 0,
                rosPort = 0
              } graph
