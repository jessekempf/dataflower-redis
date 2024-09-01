{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ImpredicativeTypes  #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE LiberalTypeSynonyms #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
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
import qualified Data.Char                        as Char
import           Data.Foldable                    (toList)
import           Data.Functor                     (($>))
import           Data.Int                         (Int64)
import           Data.Map                         (Map)
import qualified Data.Map                         as Map
import           Data.Set                         (Set)
import qualified Data.Set                         as Set
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import           Data.Text.Encoding               (decodeUtf8,
                                                   decodeUtf8Lenient, encodeUtf8)
import           Data.Void                        (Void)
import           Prelude                          hiding (take, takeWhile)
import qualified System.FilePath.Glob             as Glob
import           Test.QuickCheck                  (Arbitrary (..))
import qualified Test.QuickCheck.Gen              as Gen
import           Test.QuickCheck.Gen              (Gen)
import           Text.Printf                      (printf)
import           Text.Read                        (readMaybe)
import Debug.Trace (traceM)

data RedisProtocol a where
  RedisProtoSET     :: { setKey :: ByteString, setValue :: ByteString } -> RedisProtocol (Maybe (Either Text ByteString))
  RedisProtoGET     :: { getKey :: ByteString }                         -> RedisProtocol (Maybe ByteString)
  RedisProtoMGET    :: { mgetKey :: [ByteString] }                      -> RedisProtocol [Maybe ByteString]
  RedisProtoKEYS    :: { keysPattern :: Glob.Pattern }                  -> RedisProtocol [ByteString]
  RedisProtoEXISTS  :: { existsKey :: [ByteString] }                    -> RedisProtocol Integer
  -- RedisProtoHSET    :: { hsetKey :: ByteString, hsetFields :: NonEmpty [(ByteString, ByteString)] } -> RedisProtocol f Integer
  -- RedisProtoHGET    :: { hgetKey :: ByteString, hgetField :: ByteString }                           -> RedisProtocol f ByteString
  RedisProtoHGETALL :: { hgetallKey :: ByteString }                     -> RedisProtocol (Map RESPValue RESPValue)
  -- RedisProtoHMGET   :: { hmgetKey :: ByteString, hmgetFields :: NonEmpty [ByteString] }             -> RedisProtocol f [ByteString]

data RedisProtocolAction = forall a. (Show a, ToRESP a) =>
    RedisProtocolIntercept (RedisProtocol a)
  | RedisProtocolPassthrough [ByteString]

deriving instance Show (RedisProtocol a)
deriving instance Show RedisProtocolAction

newtype RESPBuilder a = RESPBuilder { buildRESP :: Builder }

class ToRESP a where
  toRESP :: a -> Builder

instance ToRESP a => ToRESP [a] where
  toRESP foldable =
    Builder.word8 0x2a -- *
    <> Builder.string8 (show $ length foldable)
    <> term
    <> mconcat (map toRESP $ toList foldable)
    where
      term = Builder.int16BE 0x0d0a -- \r\n

instance ToRESP (Map RESPValue RESPValue) where
  toRESP = toRESP . RESPMap

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

instance ToRESP Integer where
  toRESP = toRESP . RESPBignum

instance FromRESP Integer where
  fromRESP = fromRESP >>= \case
                (RESPInteger i) -> pure $ fromIntegral i
                (RESPBignum i)  -> pure i
                other           -> fail (printf "expected integral value, got %s" (show other))

instance ToRESP Void where
  toRESP _ = Builder.byteString ""
instance ToRESP String where
  toRESP str = Builder.char8 '+' <> Builder.string8 str <> Builder.int16BE 0x0d0a

instance ToRESP Glob.Pattern where
  toRESP glob = toRESP (encodeUtf8 $ Text.pack $ Glob.decompile glob)

arrayParser :: FromRESP a => Parser [a]
arrayParser = do
    len <- asciiEncodedInt <* string "\r\n"
    if len >= 0 then
      count len fromRESP
    else
      fail (printf "received invalid length %d" len)

instance FromRESP a => FromRESP [a] where
  fromRESP = string "*" *> arrayParser

instance FromRESP (Map RESPValue RESPValue) where
  fromRESP = fromRESP >>= \case
              (RESPMap m) -> pure m
              other -> fail (printf "expected map, got %s" (show other))
instance FromRESP RedisProtocolAction where
  fromRESP = redisParser2

redisParser2 :: Parser RedisProtocolAction
redisParser2 = do
    request@(bscmd : arguments) <- fromRESP @[ByteString]

    let cmd = Text.toUpper $ decodeUtf8Lenient bscmd

    return $ case (cmd, arguments) of
                    ("GET", [key])        -> RedisProtocolIntercept (RedisProtoGET key)
                    ("SET", [key, value]) -> RedisProtocolIntercept (RedisProtoSET key value)
                    ("KEYS", [glob])      -> RedisProtocolIntercept (RedisProtoKEYS $ Glob.compile . Data.Bytestring.UTF8.toString $ glob)
                    ("EXISTS", keys)      -> RedisProtocolIntercept (RedisProtoEXISTS keys)
                    ("HGETALL", [key])    -> RedisProtocolIntercept (RedisProtoHGETALL key)
                    ("MGET", keys)        -> RedisProtocolIntercept (RedisProtoMGET keys)
                    _                     -> RedisProtocolPassthrough request

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
    traceM "DECODING BYTESTRING"
    fromRESP >>= \case
      RESPBulkString bs -> pure bs
      unexpected -> fail $ printf "did not expect %s, expected bulk string only" (show unexpected)

instance FromRESP (Maybe ByteString) where
  fromRESP =
    fromRESP >>= \case
      RESPBulkString bs -> pure (Just bs)
      RESPNull -> pure Nothing
      unexpected -> fail $ printf "did not expect %s, expected bulk string or null only" (show unexpected)
instance {-# OVERLAPPABLE #-} FromRESP a => FromRESP (Maybe a) where
  fromRESP = (string "_" $> Nothing) <|> (Just <$> fromRESP)

instance FromRESP (Either Text ByteString) where
  fromRESP =
    fromRESP >>= \case
      (RESPSimpleString str) -> return $ Left str
      (RESPBulkString bytes) -> return $ Right bytes
      unexpected -> fail $ printf "did not expect: %s, expected simple string or bulk string only" (show unexpected)

instance ToRESP (Either Text ByteString) where
  toRESP (Left t)  = toRESP (RESPSimpleString t)
  toRESP (Right b) = toRESP (RESPBulkString b)

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
    <|> (string "_" <|> string "*-1" <|> "$-1")  $> RESPNull                          <* string "\r\n"
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
        if len >= 0 then
          take len
        else
          fail (printf "received invalid length %d" len)

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
