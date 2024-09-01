{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}
{-# LANGUAGE TypeApplications    #-}

module Net.Redis.Server where
import qualified Codec.Binary.UTF8.Generic      as ByteArray
import           Control.Applicative            ((<|>))
import           Control.Concurrent             (forkFinally, forkIO)
import           Control.Concurrent.STM         (TMVar, atomically,
                                                 modifyTVar,
                                                 newEmptyTMVarIO,
                                                 newTQueueIO, newTVarIO,
                                                 putTMVar, readTMVar,
                                                 readTQueue, readTVarIO, writeTQueue)
import           Control.Concurrent.STM.TQueue  (TQueue)
import           Control.Concurrent.STM.TVar    (TVar)
import           Control.Exception              (Exception, catch, throw)
import           Control.Monad                  (forM_, forever, void)
import           Control.Monad.IO.Class         (MonadIO (..))
import           Data.Attoparsec.ByteString
import           Data.ByteString                (ByteString)
import qualified Data.ByteString                as ByteString
import           Data.ByteString.Builder        (toLazyByteString)
import qualified Data.ByteString.UTF8           as Data.Bytestring.UTF8
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import qualified Data.Text                      as Text
import           Dataflow                       (Graph, Input, Phase (..),
                                                 Program, Vertex, output,
                                                 prepare, start, submit)
import           GHC.IO.Handle                  (BufferMode (NoBuffering),
                                                 hSetBuffering)
import           GHC.IO.StdHandles              (stdout)
import           Net.Redis.Protocol
import           Network.Socket
import           Network.Socket.ByteString      (recv)
import           Network.Socket.ByteString.Lazy (sendAll)
import qualified System.FilePath.Glob           as Glob
import           System.Posix.Signals
import           Text.Printf                    (printf)
import Data.Maybe (fromMaybe)

data RedisInputEvent = RedisSet ByteString ByteString deriving (Eq, Show)
data RedisOutputEvent =
    RedisScalar ByteString ByteString
  | RedisHashSet ByteString (Map RESPValue RESPValue)
  | RedisHashUpdate ByteString (Map RESPValue RESPValue -> Map RESPValue RESPValue)

data RedisOriginServer = RedisOriginServer {
  rosAddress :: SockAddr,
  rosPort    :: PortNumber
} deriving Show

data RedisKV = RedisKV {
  scalars :: Map ByteString ByteString,
  hashes  :: Map ByteString (Map RESPValue RESPValue)
} deriving Show

redisOutputVertex :: TVar RedisKV -> Graph (Vertex RedisOutputEvent)
redisOutputVertex register =
  output (\events ->
    forM_ events $ \event -> do
      case event of
        RedisScalar key value -> modifyTVar register (\RedisKV{..} -> RedisKV { scalars = Map.insert key value scalars, .. } )
        RedisHashSet hash value -> modifyTVar register (\RedisKV{..} -> RedisKV { hashes = Map.insert hash value hashes, .. } )
        RedisHashUpdate hash update -> modifyTVar register (\RedisKV{..} ->
          RedisKV { hashes = Map.insert hash (update $ fromMaybe Map.empty (Map.lookup hash hashes)) hashes
                    , ..
                  }
          )
  )

dataflowEngine :: MonadIO io => TVar RedisKV -> Program 'Running RedisInputEvent -> RedisProtocol a -> io (a, Program 'Running RedisInputEvent)
dataflowEngine register program command =
  case command of
    RedisProtoSET k v -> do
      program' <- submit [RedisSet k v] program
      return (Just $ Left "OK", program')
    RedisProtoGET k -> do
      mbVal <- (k `Map.lookup`) . scalars <$> liftIO (readTVarIO register)
      return (mbVal, program)
    RedisProtoMGET ks -> do
      redisKV <- liftIO $ readTVarIO register
      return (map (`Map.lookup` scalars redisKV) ks, program)
    RedisProtoKEYS keyGlob -> do
      redisKV <- liftIO $ readTVarIO register
      return (
        filter
          ((keyGlob `Glob.match`) . Data.Bytestring.UTF8.toString)
          (Map.keys (scalars redisKV) ++ Map.keys (hashes redisKV)),
          program
        )
    RedisProtoEXISTS keys -> do
      redisKV <- liftIO $ readTVarIO register
      return (fromIntegral . length $ filter
                (\k -> Map.member k (scalars redisKV)
                    || Map.member k (hashes redisKV)) keys, program)
    RedisProtoHGETALL key -> do
      redisKV <- liftIO $ readTVarIO register
      return (Map.findWithDefault Map.empty key $ hashes redisKV, program)

redisDelegate :: MonadIO io => RedisOriginSocket -> RedisProtocol a -> io a
redisDelegate redisSocket command =
  case command of
    RedisProtoSET k v     -> sendRequest redisSocket ["SET", k, v]
    RedisProtoGET k       -> sendRequest redisSocket ["GET", k]
    RedisProtoMGET ks     -> sendRequest redisSocket ("MGET" : ks)
    RedisProtoKEYS glob   -> sendRequest redisSocket ["KEYS", glob]
    RedisProtoEXISTS keys -> sendRequest redisSocket ("EXISTS" : keys)
    RedisProtoHGETALL key -> Map.fromList . pair <$> sendRequest redisSocket ["HGETALL", key]

  where
      pair :: [a] -> [(a, a)]
      pair [first, second]         = [(first, second)]
      pair (first : second : rest) = (first, second) : pair rest
      pair []                      = []
      pair _                       = undefined

dispatch :: MonadIO io => TVar RedisKV -> Program 'Running RedisInputEvent -> RedisOriginSocket -> RedisProtocol a -> io (a, Program 'Running RedisInputEvent)
dispatch register program origin command =
  case command of
    cmd@RedisProtoSET{} -> do
      (dfRetval, p') <- dataflowEngine register program cmd
      rdRetval <- redisDelegate origin cmd

      let r = case (dfRetval, rdRetval) of
                (Nothing, v) -> v
                (v, Nothing) -> v
                (Just (Left "OK"), Just (Left "OK")) -> Just (Left "OK")
                (Just (Left x), Just (Left "OK")) -> Just (Left x)
                (Just (Left "OK"), Just (Left x)) -> Just (Left x)
                (Just (Left "OK"), Just (Right old)) -> Just (Right old)
                unhandled -> throw $ RESPException (printf "Unhandled set response values: %s" (show unhandled))

      return (r, p')

    cmd@RedisProtoGET{} -> do
      (dfRetval, p') <- dataflowEngine register program cmd
      rdRetval <- redisDelegate origin cmd

      return (dfRetval <|> rdRetval, p')

    cmd@RedisProtoMGET{} -> do
      (dfRetval, p') <- dataflowEngine register program cmd
      rdRetval <- redisDelegate origin cmd

      return (zipWith (<|>) dfRetval rdRetval, p')

    cmd@RedisProtoKEYS{} -> do
      liftIO $ printf "dispatch %s to dataflow\n" (show cmd)

      (dfRetval, p') <- dataflowEngine register program cmd

      liftIO $ printf "dispatch %s to redis\n" (show cmd)

      rdRetval <- redisDelegate origin cmd

      liftIO $ printf "flower: %s; redis: %s\n" (show dfRetval) (show rdRetval)

      return (dfRetval ++ rdRetval, p')

    cmd@RedisProtoEXISTS{} -> do
      (dfRetval, p') <- dataflowEngine register program cmd
      rdRetval <- redisDelegate origin cmd

      return (dfRetval + rdRetval, p')

    cmd@RedisProtoHGETALL{} -> do
      (dfRetval, p') <- dataflowEngine register program cmd
      rdRetval <- redisDelegate origin cmd

      return (dfRetval `Map.union` rdRetval, p')

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

newtype RedisClientSocket = RedisClientSocket { rcsSocket :: Socket } deriving Show

newtype RESPException = RESPException String deriving (Eq, Show)

instance Exception RESPException

sendRequest :: (MonadIO io, ToRESP a, FromRESP b) => RedisOriginSocket -> a -> io b
sendRequest = sendRequestRaw fromRESP

sendRequestRaw :: (MonadIO io, ToRESP a) => Parser b -> RedisOriginSocket -> a -> io b
sendRequestRaw parser RedisOriginSocket{..} request = do
  liftIO $ sendAll redisSocket $ toLazyByteString $ toRESP request
  parseWith (liftIO $ recv redisSocket 4096) parser ByteString.empty >>= \case
    (Done _ b)        -> return b
    Fail input _ msg  -> throw (RESPException $ printf "sendRequest: error when parsing %s: %s" (show input) msg)
    Partial _         -> throw (RESPException "Not enough data received for full decode")

recvRequest :: (MonadIO io, FromRESP a) => RedisClientSocket -> io a
recvRequest RedisClientSocket{..} =
  parseWith (liftIO $ recv rcsSocket 4096) fromRESP ByteArray.empty >>= \case
    (Done _ b)        -> return b
    Fail input _ msg  -> throw (RESPException $ printf "recvRequest: error when parsing %s: %s" (show input) msg)
    Partial _         -> throw (RESPException "Not enough data received for full decode")


sendResponse :: (MonadIO io, ToRESP a) => RedisClientSocket -> a -> io ()
sendResponse RedisClientSocket{..} response =
  liftIO $ sendAll rcsSocket $ toLazyByteString $ toRESP response

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

  printf "Initializing dataflow processor\n"
  originSocket <- redisOriginSocket RedisOriginServer{..}
  requestQueue <- newTQueueIO
  void . forkIO $
    serviceLoop redisKV originSocket requestQueue dataflowerGraph

  printf "Starting Dataflower-Redis server on %s\n" (show redisSocket)


  forever $ do
    (sessionSocket, peer)  <- accept redisSocket
    printf "Received client connection on %s from %s\n" (show sessionSocket) (show peer)

    forkFinally
      (redisSession (RedisClientSocket sessionSocket) originSocket requestQueue)
      (\result -> do
        printf "Shutting down handler[%s]: handler produced %s\n" (show (sessionSocket, peer)) (show result)
        gracefulClose sessionSocket 5000
      )

  where
    serviceLoop :: TVar RedisKV -> RedisOriginSocket -> TQueue ServiceRequest -> Program 'Running RedisInputEvent -> IO ()
    serviceLoop kv origin inputQueue prog = do
      liftIO (atomically $ readTQueue inputQueue) >>= \case
        ServiceRequest{..} -> do
          printf "  service loop: received %s\n" (show srAction)

          serviceLoop kv origin inputQueue =<< catch (
            do
              (result, prog') <- dispatch kv prog origin srAction

              printf "  service loop: ACTION: %s => %s\n" (show srAction) (show result)

              atomically $
                putTMVar srResult (Right result)

              return prog'
            ) (\e ->
            do
              atomically $
                putTMVar srResult (Left e)
              return prog
            )

    redisSession :: RedisClientSocket -> RedisOriginSocket -> TQueue ServiceRequest -> IO ()
    redisSession sock origin requestQueue = forever $ do
      parseResult <- recvRequest sock
      printf "parse result => %s\n" (show parseResult)

      case parseResult of
        RedisProtocolIntercept action -> do
          printf "[%s] ACTION: %s\n" (show sock) (show action)

          resultVar <- newEmptyTMVarIO

          atomically $ do
            writeTQueue requestQueue (ServiceRequest action resultVar)

          result <- atomically $ readTMVar resultVar

          printf "[%s] ACTION: %s => %s\n" (show sock) (show action) (show result)

          case result of
            Left exc  -> sendResponse sock (RESPSimpleError $ Text.pack (show exc))
            Right val -> sendResponse sock val

        RedisProtocolPassthrough command -> do
          resp :: RESPValue <- sendRequest origin command
          printf "UNINTERCEPTED  %s -> %s\n" (show command) (show resp)

          sendResponse sock resp
data ServiceRequest = forall a. (Show a, ToRESP a) => ServiceRequest {
  srAction :: RedisProtocol a,
  srResult :: TMVar (Either RESPException a)
}
