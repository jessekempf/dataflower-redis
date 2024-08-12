{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ImpredicativeTypes  #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE LiberalTypeSynonyms #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

module Net.Redis.Protocol where

import           Control.Applicative              ((<|>))
import           Data.Attoparsec.ByteString
import           Data.Attoparsec.ByteString.Char8 (char)
import qualified Data.Attoparsec.ByteString.Char8 as Char8
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as ByteString
import           Data.ByteString.Builder          (Builder)
import qualified Data.ByteString.Builder          as Builder
import qualified Data.ByteString.UTF8             as Data.Bytestring.UTF8
import           Data.Foldable                    (toList)
import           Data.Functor                     (($>))
import           Data.Int                         (Int64)
import           Data.Kind                        (Type)
import           Data.Map                         (Map)
import qualified Data.Map
import qualified Data.Map                         as Map
import           Data.Set                         (Set)
import qualified Data.Set                         as Set
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import           Data.Text.Encoding               (decodeUtf8)
import           Network.Socket                   (Family (AF_802, AF_INET))
import           Prelude                          hiding (take, takeWhile)
import           System.FilePath.Glob             (Pattern, compile)
import           Text.Read                        (readMaybe)
import Test.QuickCheck (Arbitrary (..))
import qualified Test.QuickCheck.Gen as Gen
import Test.QuickCheck.Gen (Gen)
import qualified Data.Char as Char
import Control.Monad (void)

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
                  | RedisCommandUnintercepted [ByteString]
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

arrayParser :: FromRESP a => Parser [a]
arrayParser = do
    len <- asciiEncodedInt <* string "\r\n"
    count len fromRESP

redisParser :: Parser RedisCommand
redisParser = do
  (requestArray :: [ByteString]) <- char '*' *> arrayParser

  return $ case requestArray of
    -- ["GET", key]        -> RedisCommandGet key
    -- ["SET", key, value] -> RedisCommandSet key value
    -- "MGET" : keys       -> RedisCommandMultiGet keys
    -- ["KEYS", globstr]   -> RedisCommandKeys (compile $ Data.Bytestring.UTF8.toString globstr)
    unintercepted       -> RedisCommandUnintercepted unintercepted

redisResponseParser :: ToRESP a => Parser a
redisResponseParser = undefined

bulkString :: ByteString -> Parser ByteString
bulkString input = string $ ByteString.toStrict . Builder.toLazyByteString . toRESP $ input

asciiEncodedInt :: forall i. Integral i => Parser i
asciiEncodedInt = digits <|> ("+" *> digits) <|> ("-" *> ((\x -> -x) <$> digits))
  where
    digits :: Parser i
    digits = integerize <$> many1 asciiDigits

    asciiDigits :: Parser i
    asciiDigits = satisfyWith (\w -> fromIntegral w - 0x30) (\w -> 0 <= w && w <= 9)

    integerize :: [i] -> i
    integerize = snd . foldr (\digit (power, accumulator) -> (power * 10, accumulator + digit * power)) (1, 0)


class FromRESP t where
  fromRESP :: Parser t

instance FromRESP ByteString where
  fromRESP = do
    (RESPBulkString bs) <- fromRESP
    return bs
data RESPPrimitive =
    RESPSimpleString Text
  | RESPSimpleError Text
  | RESPInteger Int64
  | RESPBulkString ByteString
  | RESPNull
  | RESPBoolean Bool
  | RESPDouble Double
  | RESPBignum Integer
  | RESPBulkError ByteString
  | RESPVerbatimString ByteString ByteString
  deriving (Eq, Ord, Show)

instance Arbitrary RESPPrimitive where
  arbitrary :: Gen RESPPrimitive
  arbitrary = Gen.oneof [
      RESPSimpleString . Text.pack <$> (arbitrary `Gen.suchThat` all (\x -> Char.isAscii x && not (Char.isControl x))),
      RESPSimpleError . Text.pack <$> (arbitrary `Gen.suchThat` all (\x -> Char.isAscii x && not (Char.isControl x))),
      RESPInteger <$> arbitrary,
      RESPBulkString . ByteString.pack <$> arbitrary,
      pure RESPNull,
      RESPBoolean <$> arbitrary,
      RESPDouble <$> arbitrary,
      RESPBignum <$> arbitrary,
      RESPBulkError . ByteString.pack <$> arbitrary,
      RESPVerbatimString . ByteString.pack <$> Gen.vectorOf 3 arbitrary <*> (ByteString.pack <$> arbitrary)
    ]

instance ToRESP RESPPrimitive where
  toRESP (RESPSimpleString ss)            = Builder.char8 '+' <> Builder.string8 (Text.unpack ss) <> Builder.word16BE 0x0d0a
  toRESP (RESPSimpleError se)             = Builder.char8 '-' <> Builder.string8 (Text.unpack se) <> Builder.word16BE 0x0d0a
  toRESP (RESPInteger i)                  = Builder.char8 ':' <> Builder.string8 (show i) <> Builder.word16BE 0x0d0a
  toRESP (RESPBulkString bs)              = Builder.char8 '$'
                                            <> Builder.string8 (show $ ByteString.length bs) <> Builder.int16BE 0x0d0a
                                            <> Builder.byteString bs <> Builder.int16BE 0x0d0a
  toRESP RESPNull                         = Builder.char8 '_' <> Builder.word16BE 0x0d0a
  toRESP (RESPBoolean True)               = Builder.byteString "#t\r\n"
  toRESP (RESPBoolean False)              = Builder.byteString "#f\r\n"
  toRESP (RESPDouble f)                   = Builder.char8 ',' <> Builder.string8 (show f) <> Builder.int16BE 0x0d0a
  toRESP (RESPBignum i)                   = Builder.char8 '(' <> Builder.string8 (show i) <> Builder.int16BE 0x0d0a
  toRESP (RESPBulkError be)               = Builder.char8 '!'
                                            <> Builder.string8 (show $ ByteString.length be) <> Builder.int16BE 0x0d0a
                                            <> Builder.byteString be <> Builder.int16BE 0x0d0a
  toRESP (RESPVerbatimString typ content) = Builder.char8 '='
                                            <> Builder.string8 (show $ 4 + ByteString.length content) <> Builder.int16BE 0x0d0a
                                            <> Builder.byteString typ <> Builder.char8 ':'
                                            <> Builder.byteString content <> Builder.int16BE 0x0d0a

instance FromRESP RESPPrimitive where
  fromRESP :: Parser RESPPrimitive
  fromRESP =
        char '+'    *> (RESPSimpleString . decodeUtf8 <$> Char8.takeWhile (/= '\r'))  <* string "\r\n"
    <|> char '-'    *> (RESPSimpleError . decodeUtf8 <$> Char8.takeWhile (/= '\r'))   <* string "\r\n"
    <|> char ':'    *> (RESPInteger <$> asciiEncodedInt)                              <* string "\r\n"
    <|> char '$'    *> (RESPBulkString <$> byteStringReader)                          <* string "\r\n"
    <|> string "_"  $> RESPNull                                                       <* string "\r\n"
    <|> string "#t" $> RESPBoolean True                                               <* string "\r\n"
    <|> string "#f" $> RESPBoolean False                                              <* string "\r\n"
    <|> char ','    *> (RESPDouble <$> parseDouble)                                   <* string "\r\n"
    <|> char '('    *> (RESPBignum <$> asciiEncodedInt)                               <* string "\r\n"
    <|> char '!'    *> (RESPBulkError <$> byteStringReader)                           <* string "\r\n"
    <|> char '='    *> (uncurry RESPVerbatimString <$> verbatimStringReader)          <* string "\r\n"

    where
      byteStringReader :: Parser ByteString
      byteStringReader = do
        len <- asciiEncodedInt <* string "\r\n"
        take len

      parseDouble :: Parser Double
      parseDouble = do
        str <- Text.unpack . decodeUtf8 <$> Char8.takeWhile (/= '\r')
        case readMaybe @Double str of
          Nothing -> fail "not a redis-encoded double"
          Just  d -> return d


      verbatimStringReader :: Parser (ByteString, ByteString)
      verbatimStringReader = do
        bs <- byteStringReader
        return (ByteString.take 3 bs, ByteString.drop 4 bs)

data RESPValue =
    RESPPrimitive' RESPPrimitive
  | RESPArray [RESPValue]
  | RESPMap (Map RESPValue RESPValue)
  | RESPSet (Set RESPValue)
  deriving (Eq, Ord, Show)

instance Arbitrary RESPValue where
  arbitrary :: Gen RESPValue
  arbitrary = Gen.oneof [
      RESPPrimitive' <$> arbitrary,
      RESPArray <$> Gen.listOf recursive,
      RESPMap . Map.fromList <$> (zip <$> Gen.listOf recursive <*> Gen.listOf recursive),
      RESPSet . Set.fromList <$> Gen.listOf recursive
    ]
    where
      recursive = Gen.frequency [
          (197, RESPPrimitive' <$> arbitrary),
          (1, RESPArray <$> Gen.listOf recursive),
          (1, RESPMap . Map.fromList <$> (zip <$> Gen.listOf (RESPPrimitive' <$> arbitrary) <*> Gen.listOf recursive)),
          (1, RESPSet . Set.fromList <$> Gen.listOf recursive)
        ]
  
instance ToRESP RESPValue where
  toRESP (RESPPrimitive' p) = toRESP p
  toRESP (RESPArray values) = Builder.char8 '*'
                              <> Builder.string8 (show $ length values) <> Builder.int16BE 0x0d0a
                              <> mconcat (map toRESP values)
  toRESP (RESPMap kv)       = Builder.char8 '%'
                              <> Builder.string8 (show $ length kv) <> Builder.int16BE 0x0d0a
                              <> mconcat (map (\(k, v) -> toRESP k <> toRESP v) $ Map.toList kv)
  toRESP (RESPSet set)      = Builder.char8 '~'
                              <> Builder.string8 (show $ length set) <> Builder.int16BE 0x0d0a
                              <> mconcat (map toRESP $ Set.toList set)


instance FromRESP RESPValue where
  fromRESP :: Parser RESPValue
  fromRESP =
        RESPPrimitive' <$> fromRESP
    <|> char '*' *> (RESPArray <$> arrayParser)
    <|> char '%' *> (RESPMap <$> mapParser)
    <|> char '~' *> (RESPSet <$> setParser)

    where
      mapParser :: Parser (Map RESPValue RESPValue)
      mapParser = do
        len <- asciiEncodedInt <* string "\r\n"
        Map.fromList . pair <$> count (2 * len) fromRESP

      setParser :: Parser (Set RESPValue)
      setParser = Set.fromList <$> arrayParser

      pair :: [a] -> [(a, a)]
      pair [first, second]         = [(first, second)]
      pair (first : second : rest) = (first, second) : pair rest
      pair []                      = []
      pair _                       = undefined
