{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

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
import qualified System.FilePath.Glob           as Glob
import           System.Posix.Signals
import           Text.Printf                    (printf)

redisStmEval :: TVar (Map ByteString ByteString) -> RedisProtocol STM a -> STM a
redisStmEval keyspaceVar (RedisProtoGET key)       = readTVar keyspaceVar <&> (\m -> key `Map.lookup` m)
redisStmEval keyspaceVar (RedisProtoSET key value) = modifyTVar' keyspaceVar (Map.insert key value)
redisStmEval keyspaceVar (RedisProtoMGET keys)     = readTVar keyspaceVar <&> (\m -> map (`Map.lookup` m) keys)
redisStmEval keyspaceVar (RedisProtoKEYS keyGlob)  = readTVar keyspaceVar <&> (filter ((keyGlob `Glob.match`) . show) . Map.keys)

server :: IO ()
server = do
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

    forkFinally (redisSession sessionSocket tvar) (const $ gracefulClose sessionSocket 5000)

  where
    redisSession sock tvar = do
      parseResult <- parseWith (recv sock 4096) redisParser ByteString.empty
      print parseResult

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
            RedisUsageError err -> do
              send sock $ toLazyByteString (toRESP err)
            RedisCommandCommandDocs ->
              send sock "%0\r\n"
            RedisCommandPing Nothing ->
              send sock "+PONG\r\n"
            RedisCommandPing (Just response) ->
              send sock $ toLazyByteString (toRESP response)
            RedisCommandUnsupported (cmd : _) ->
              send sock ("-ERR unknown command " <> ByteString.fromStrict cmd)
            RedisCommandUnsupported _ ->
              send sock "-ERR command not provided, client drunk?"
          redisSession sock tvar
        other -> print other
