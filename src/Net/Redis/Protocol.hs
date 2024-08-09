{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ImpredicativeTypes  #-}
{-# LANGUAGE LiberalTypeSynonyms #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}
{-# LANGUAGE TypeFamilies        #-}

module Net.Redis.Protocol where

import           Data.Attoparsec.ByteString
import           Data.ByteString            (ByteString)
import qualified Data.ByteString            as ByteString
import           Data.ByteString.Builder    (Builder)
import qualified Data.ByteString.Builder    as Builder
import qualified Data.ByteString.UTF8       as Data.Bytestring.UTF8
import           Data.Foldable              (toList)
import           Data.Functor               (($>))
import           Data.Kind                  (Type)
import           Prelude                    hiding (take)
import           System.FilePath.Glob       (Pattern, compile)

data RedisProtocol (f :: Type -> Type) a where
  RedisProtoSET     :: { setKey :: ByteString, setValue :: ByteString } -> RedisProtocol f ()
  RedisProtoGET     :: { getKey :: ByteString }                         -> RedisProtocol f (Maybe ByteString)
  RedisProtoMGET    :: { mgetKey :: [ByteString] }                      -> RedisProtocol f [Maybe ByteString]
  RedisProtoKEYS    :: { keysPattern :: Pattern }                       -> RedisProtocol f [ByteString]
  -- RedisProtoHSET    :: { hsetKey :: ByteString, hsetFields :: NonEmpty [(ByteString, ByteString)] } -> RedisProtocol f Integer
  -- RedisProtoHGET    :: { hgetKey :: ByteString, hgetField :: ByteString }                           -> RedisProtocol f ByteString
  -- RedisProtoHGETALL :: { hgetallKey :: ByteString }                                                 -> RedisProtocol f [(ByteString, ByteString)]
  -- RedisProtoHMGET   :: { hmgetKey :: ByteString, hmgetFields :: NonEmpty [ByteString] }             -> RedisProtocol f [ByteString]

data RedisCommand = RedisCommandGet ByteString
                  | RedisCommandSet ByteString ByteString
                  | RedisCommandMultiGet [ByteString]
                  | RedisCommandKeys Pattern
                  | RedisCommandPing (Maybe ByteString)
                  | RedisCommandCommandDocs
                  | RedisUsageError String
                  | RedisCommandUnsupported [ByteString]
                  deriving (Eq, Show)

newtype RESPBuilder a = RESPBuilder { buildRESP :: Builder }

class ToRESP a where
  toRESP :: a -> Builder

instance {-# OVERLAPPABLE #-} (Foldable f, ToRESP a) => ToRESP (f a) where
  toRESP foldable =
    Builder.word8 0x2a -- *
    <> Builder.string8 (show $ length foldable)
    <> term
    <> mconcat (map toRESP $ toList foldable)
    where
      term = Builder.int16BE 0x0d0a -- \r\n

instance ToRESP a => ToRESP (Maybe a) where
  toRESP Nothing  = Builder.string8 "_\r\n"
  toRESP (Just a) = toRESP a

instance ToRESP ByteString where
  toRESP input =
    Builder.word8 0x24 -- $
      <> Builder.string8 (show $ ByteString.length input)
      <> term
      <> Builder.byteString input
      <> term
    where
      term = Builder.int16BE 0x0d0a -- \r\n

instance ToRESP String where
  toRESP str = Builder.char8 '+' <> Builder.string8 str <> Builder.int16BE 0x0d0a

parseRedisCommand :: ByteString -> Either String RedisCommand
parseRedisCommand = parseOnly redisParser

redisParser :: Parser RedisCommand
redisParser = do
  requestArray <- arrayParser bulkStringParser

  return $ case requestArray of
    ["GET", key]        -> RedisCommandGet key
    ["SET", key, value] -> RedisCommandSet key value
    "MGET" : keys       -> RedisCommandMultiGet keys
    ["KEYS", globstr]   -> RedisCommandKeys (compile $ Data.Bytestring.UTF8.toString globstr)
    ["KEYS"]            -> RedisUsageError "ERR wrong number of arguments for command"
    ["PING"]            -> RedisCommandPing Nothing
    ["PING", value]     -> RedisCommandPing (Just value)
    ["COMMAND", "DOCS"] -> RedisCommandCommandDocs
    unsupported         -> RedisCommandUnsupported unsupported

bulkString :: ByteString -> Parser ByteString
bulkString input = string $ ByteString.toStrict . Builder.toLazyByteString . toRESP $ input

bulkStringParser :: Parser ByteString
bulkStringParser =
  word8 0x24 *> -- $
  do
    len <- asciiEncodedInt <* terminator
    take len <* terminator

arrayParser :: Parser a -> Parser [a]
arrayParser p =
  word8 0x2a *> -- *
  do
    len <- asciiEncodedInt <* terminator
    count len p

terminator :: Parser ()
terminator = (word8 0x0d *> word8 0x0a) $> () -- \r\n

asciiEncodedInt :: Parser Int
asciiEncodedInt = integerize <$> many1 asciiDigits
  where
    asciiDigits :: Parser Int
    asciiDigits = satisfyWith (\w -> fromIntegral w - 0x30) (\w -> 0 <= w && w <= 9)

    integerize :: [Int] -> Int
    integerize = snd . foldr (\digit (power, accumulator) -> (power * 10, accumulator + digit * power)) (1, 0)
