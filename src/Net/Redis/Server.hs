{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}
{-# LANGUAGE TypeApplications #-}

module Net.Redis.Server where
import           Control.Concurrent             (forkFinally)
import           Control.Concurrent.STM         (STM, atomically, newTVarIO)
import           Control.Concurrent.STM.TVar    (TVar, modifyTVar', readTVar)
import           Control.Monad                  (forever, void)
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
import qualified Network.Socket.ByteString.Lazy as LazySocket
import qualified System.FilePath.Glob           as Glob
import           System.Posix.Signals
import           Text.Printf                    (printf)

redisStmEval :: TVar (Map ByteString ByteString) -> RedisProtocol STM a -> STM a
redisStmEval keyspaceVar (RedisProtoGET key)       = readTVar keyspaceVar <&> (\m -> key `Map.lookup` m)
redisStmEval keyspaceVar (RedisProtoSET key value) = modifyTVar' keyspaceVar (Map.insert key value)
redisStmEval keyspaceVar (RedisProtoMGET keys)     = readTVar keyspaceVar <&> (\m -> map (`Map.lookup` m) keys)
redisStmEval keyspaceVar (RedisProtoKEYS keyGlob)  = readTVar keyspaceVar <&> (filter ((keyGlob `Glob.match`) . show) . Map.keys)

data RedisOriginServer = RedisOriginServer {
  rosAddress :: SockAddr,
  rosPort    :: PortNumber
} deriving Show

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
  void $ send redisSocket $ toLazyByteString $ toRESP request

  parseWith (recv redisSocket 4096) fromRESP ByteString.empty

server :: RedisOriginServer -> IO ()
server RedisOriginServer{..} = do
  hSetBuffering stdout NoBuffering

  printf "Bringing up Dataflower-Redis...\n"

  redisSocket <- socket AF_INET6 Stream 0
  bind redisSocket (SockAddrInet6 6379 0 (0, 0, 0, 0) 0)
  tvar <- newTVarIO Map.empty

  listen redisSocket 1024

  void $ installHandler keyboardSignal (Catch (close redisSocket)) Nothing

  printf "Starting Dataflower-Redis server on %s\n" (show redisSocket)

  forever $ do
    (sessionSocket, peer)  <- accept redisSocket
    printf "Received client connection on %s from %s\n" (show sessionSocket) (show peer)

    originSocket <- redisOriginSocket RedisOriginServer{..}

    forkFinally (redisSession sessionSocket originSocket tvar) (const $ gracefulClose sessionSocket 5000)

  where
    redisSession :: Socket -> RedisOriginSocket -> TVar (Map ByteString ByteString) -> IO ()
    redisSession sock origin tvar = do
      parseResult <- parseWith (recv sock 4096) redisParser ByteString.empty
      printf "parse result => %s\n" (show parseResult)

      case parseResult of
        Done _ redisCommand -> do
          void $ case redisCommand of
            RedisCommandGet key -> do
              atomically (redisStmEval tvar (RedisProtoGET key)) >>= \case
                Nothing -> send sock "_\r\n"
                Just val -> send sock $ toLazyByteString (toRESP val)
            RedisCommandSet key value -> do
              atomically $ redisStmEval tvar (RedisProtoSET key value)
              send sock "+OK\r\n"
            RedisCommandMultiGet [] -> do
              send sock "-ERR MGET requires at least one key"
            RedisCommandMultiGet keys -> do
              values <- atomically (redisStmEval tvar (RedisProtoMGET keys))
              send sock $ toLazyByteString (toRESP values)
            RedisCommandKeys glob -> do
              values <- atomically (redisStmEval tvar (RedisProtoKEYS glob))
              send sock $ toLazyByteString (toRESP values)
            RedisCommandUnintercepted commandArray -> do
              printf "  received: %s\n" (show commandArray)
              resp <- sendRequest origin commandArray

              printf "  %s -> %s\n" (show commandArray) (show resp)

              case resp of
                Done _ reply -> send sock $ toLazyByteString (toRESP @RESPValue reply)
                _ -> return 0

          redisSession sock origin tvar
        other -> print other
