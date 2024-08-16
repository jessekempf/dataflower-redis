{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE DataKinds #-}

module Net.Redis.Server where
import qualified Codec.Binary.UTF8.Generic      as ByteArray
import           Control.Concurrent             (forkFinally)
import           Control.Concurrent.STM         (STM, newTVarIO, readTVarIO, modifyTVar)
import           Control.Concurrent.STM.TVar    (TVar, modifyTVar', readTVar)
import           Control.Monad                  (forever, void, forM_)
import           Data.Attoparsec.ByteString
import           Data.ByteString                (ByteString)
import qualified Data.ByteString                as ByteString
import           Data.ByteString.Builder        (toLazyByteString)
import           Data.Functor                   ((<&>))
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import           GHC.IO.Handle                  (BufferMode (NoBuffering),
                                                 hSetBuffering)
import           GHC.IO.StdHandles              (stdout)
import           Net.Redis.Protocol
import           Network.Socket
import           Network.Socket.ByteString      (recv)
import           Network.Socket.ByteString.Lazy (send)
import           System.Posix.Signals
import           Text.Printf                    (printf)
import Dataflow (Program, Phase (..), start, submit, Graph, Input, prepare, Vertex, vertex, output)
import Dataflow.Operators (statelessVertex)
import qualified System.FilePath.Glob as Glob
import qualified Data.ByteString.UTF8 as Data.Bytestring.UTF8
import Network.Socket.ByteString.Lazy (sendAll)

data RedisInputEvent = RedisSet ByteString ByteString deriving (Eq, Show)
data RedisOutputEvent = RedisScalar ByteString ByteString | RedisHash ByteString (Map RESPValue RESPValue) deriving (Eq, Show)

redisStmEval :: TVar (Map ByteString ByteString) -> RedisProtocol a -> STM a
redisStmEval keyspaceVar (RedisProtoGET key)       = readTVar keyspaceVar <&> (\m -> key `Map.lookup` m)
redisStmEval keyspaceVar (RedisProtoSET key value) = modifyTVar' keyspaceVar (Map.insert key value)
-- redisStmEval keyspaceVar (RedisProtoMGET keys)     = readTVar keyspaceVar <&> (\m -> map (`Map.lookup` m) keys)
-- redisStmEval keyspaceVar (RedisProtoKEYS keyGlob)  = readTVar keyspaceVar <&> (filter ((keyGlob `Glob.match`) . show) . Map.keys)
data RedisOriginServer = RedisOriginServer {
  rosAddress :: SockAddr,
  rosPort    :: PortNumber
} deriving Show

data RedisKV = RedisKV {
  scalars :: Map ByteString ByteString,
  hashes :: Map ByteString (Map RESPValue RESPValue)
} deriving Show

redisOutputVertex :: TVar RedisKV -> Graph (Vertex RedisOutputEvent)
redisOutputVertex register =
  output (\events ->
    forM_ events $ \event -> do
      case event of
        RedisScalar key value -> modifyTVar register (\RedisKV{..} -> RedisKV { scalars = Map.insert key value scalars, .. } )
        RedisHash key value -> modifyTVar register (\RedisKV{..} -> RedisKV { hashes = Map.insert key value hashes, .. } )
  )




newtype RedisOriginSocket = RedisOriginSocket { redisSocket :: Socket } deriving Show

redisOriginSocket :: RedisOriginServer -> IO RedisOriginSocket
redisOriginSocket RedisOriginServer{..} = do
  sock <- socket (case rosAddress of
                    SockAddrInet  {} -> AF_INET
                    SockAddrInet6 {} -> AF_INET6
                    SockAddrUnix {}  -> AF_UNIX
                  ) Stream 0

  connect sock rosAddress

  return $ RedisOriginSocket sock

sendRequest :: (ToRESP a, FromRESP b) => RedisOriginSocket -> a -> IO (Result b)
sendRequest RedisOriginSocket{..} request = do
  sendAll redisSocket $ toLazyByteString $ toRESP request
  parseWith (recv redisSocket 4096) fromRESP ByteString.empty

server :: RedisOriginServer -> (TVar RedisKV -> Graph (Input RedisInputEvent)) -> IO ()
server RedisOriginServer{..} mkGraph = do
  hSetBuffering stdout NoBuffering

  printf "Bringing up Dataflower-Redis...\n"

  redisSocket <- socket AF_INET6 Stream 0
  setSocketOption redisSocket ReuseAddr 1
  bind redisSocket (SockAddrInet6 6379 0 (0, 0, 0, 0) 0)

  listen redisSocket 1024

  void $ installHandler keyboardSignal (Catch (do
      printf "\n...shutdown signal received\n"
      close redisSocket
      printf "closed listener socket\n"
    )) Nothing

  printf "Initializing dataflower graph\n"
  redisKV <- newTVarIO (RedisKV Map.empty Map.empty)
  program <- prepare (mkGraph redisKV)
  dataflowerGraph <- start program

  printf "Starting Dataflower-Redis server on %s\n" (show redisSocket)

  forever $ do
    (sessionSocket, peer)  <- accept redisSocket
    printf "Received client connection on %s from %s\n" (show sessionSocket) (show peer)

    originSocket <- redisOriginSocket RedisOriginServer{..}

    forkFinally (redisSession sessionSocket originSocket redisKV dataflowerGraph) (const $ gracefulClose sessionSocket 5000)

  where
    redisSession :: Socket -> RedisOriginSocket -> TVar RedisKV -> Program 'Running RedisInputEvent -> IO ()
    redisSession sock origin kv prog = do
      parseResult <- parseWith (recv sock 4096) redisParser2 ByteArray.empty
      printf "parse result => %s\n" (show parseResult)

      case parseResult of
        Done _ RedisProtocolAction{..} -> do
          printf "  received: %s\n" (show rpaCommand)

          prog' <- case rpaAction of
            (Just (RedisProtoSET k v)) -> do
              p <- submit [RedisSet k v] prog

              resp <- sendRequest origin rpaCommand
              printf "SET  %s -> %s\n" (show rpaCommand) (show resp)

              void $ case resp of
                Done _ reply -> send sock $ toLazyByteString (toRESP @RESPValue reply)
                _ -> return 0
              return p

            (Just (RedisProtoGET key)) -> do
              register <- readTVarIO kv

              resp <- case key `Map.lookup` scalars register of
                Nothing -> do
                  r' <- sendRequest origin rpaCommand
                  printf "GET  %s -> %s\n" (show rpaCommand) (show r')
                  return r'
                Just val -> do
                  printf "GET  %s -dflow-> %s\n" (show rpaCommand) (show val)
                  return (Done "" $ RESPPrimitive' $ RESPBulkString val)

              void $ case resp of
                Done _ reply -> send sock $ toLazyByteString (toRESP @RESPValue reply)
                unknown -> printf "GET: Unexpected resp: %s" (show unknown) >> return 0
              return prog

            (Just (RedisProtoHGETALL key)) -> do
              register <- readTVarIO kv

              resp <- case key `Map.lookup` hashes register of
                Nothing -> do
                  r' <- sendRequest origin rpaCommand
                  printf "HGETALL  %s -> %s\n" (show rpaCommand) (show r')
                  return r'
                Just val -> do
                  printf "HGETALL  %s -dflow-> %s\n" (show rpaCommand) (show val)
                  return (Done "" $ RESPMap val)

              void $ case resp of
                Done _ reply -> send sock $ toLazyByteString (toRESP @RESPValue reply)
                unknown -> printf "GET: Unexpected resp: %s" (show unknown) >> return 0
              return prog

            (Just (RedisProtoMGET keys)) -> do
              register <- readTVarIO kv

              resp <- sendRequest origin rpaCommand

              void $ case resp of
                Done _ (RESPArray remoteValues) -> do
                  let localValues = map (`Map.lookup` scalars register) keys
                      allValues = zipWith (\local remote ->
                                              case (local, remote) of
                                                (Nothing, _) -> remote
                                                (Just x, RESPPrimitive' RESPNull) -> RESPPrimitive' $ RESPBulkString x
                                                _ -> error $ printf "can't happen: %s and %s are both present!" (show local) (show remote)
                                          ) localValues remoteValues

                  send sock $ toLazyByteString (toRESP $ RESPArray allValues)
                _ -> return 0
              return prog

            (Just (RedisProtoKEYS keyGlob)) -> do
              register <- readTVarIO kv
              resp <- sendRequest origin rpaCommand

              printf "KEYS %s -redis-> %s\n" (show rpaCommand) (show resp)
              printf "KEYS %s -dflow-> %s\n" (show rpaCommand) (show resp)

              let localKeys   = filter
                                  ((keyGlob `Glob.match`) . Data.Bytestring.UTF8.toString)
                                  (Map.keys (scalars register) ++ Map.keys (hashes register))
                  localValues = map (RESPPrimitive' . RESPBulkString) localKeys

              let resp' = case resp of
                            Done x (RESPArray a) -> Done x (RESPArray $ localValues ++ a)
                            _ -> resp

              void $ case resp' of
                Done _ reply -> send sock $ toLazyByteString (toRESP @RESPValue reply)
                _ -> return 0
              return prog

            (Just (RedisProtoEXISTS keys)) -> do
              register <- readTVarIO kv
              resp <- sendRequest origin rpaCommand

              let localExistsCount = length $ filter (\k -> Map.member k (scalars register) || Map.member k (hashes register)) keys

              void $ case resp of
                Done _ reply -> send sock $ toLazyByteString (toRESP @RESPValue reply)
                _ -> return 0              
              return prog
            Nothing -> do
              resp <- sendRequest origin rpaCommand
              printf "UNINTERCEPTED  %s -> %s\n" (show rpaCommand) (show resp)

              void $ case resp of
                Done _ reply -> send sock $ toLazyByteString (toRESP @RESPValue reply)
                _ -> return 0

              return prog

          redisSession sock origin kv prog'
        other -> print other
