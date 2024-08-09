{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Net.Redis.ProtocolSpec (spec) where

import           Data.Attoparsec.ByteString (parseOnly)
import           Data.ByteString            (ByteString)
import qualified Data.ByteString            as ByteString
import qualified Data.ByteString.Builder    as Builder
import           Data.Vector                (fromList)
import           Net.Redis.Protocol
import           Test.Hspec
import           Test.QuickCheck            hiding (discard)

newtype ArbitraryByteString = ArbitraryByteString { unArbitraryByteString :: ByteString } deriving Show

instance Arbitrary ArbitraryByteString where
  arbitrary = ArbitraryByteString . ByteString.pack <$> arbitrary

spec :: Spec
spec = do
  let toStrictByteString = ByteString.toStrict . Builder.toLazyByteString
      arrayOf = toStrictByteString . arrayBuilder

  describe "command parser" $ do
    it "parses GET correctly" $ do
      parseRedisCommand
        (arrayOf [bulkStringBuilder "GET", bulkStringBuilder "foo:3"])
      `shouldBe` Right (RedisCommandGet "foo:3")

    it "parses SET correctly" $ do
      parseRedisCommand
        (arrayOf [bulkStringBuilder "SET", bulkStringBuilder "foo:3", bulkStringBuilder "\"bar-7\""])
      `shouldBe` Right (RedisCommandSet "foo:3" "\"bar-7\"")

  describe "bulk string parser" $ do
    it "parses a correctly-formed string" $ do
      parseOnly bulkStringParser "$11\r\nhello world\r\n" `shouldBe` Right "hello world"

    it "does not parse an incorrectly-formed string" $ do
      parseOnly bulkStringParser "$10\r\nhello world\r\n" `shouldBe` Left "13: Failed reading: satisfy"
      parseOnly bulkStringParser "$12\r\nhello world\r\n" `shouldBe` Left "13: Failed reading: satisfy"

  describe "bulk string recognizer" $ do
    it "recognizes a correctly-formed string" $ do
      parseOnly (bulkString "hello world") "$11\r\nhello world\r\n" `shouldBe` Right "$11\r\nhello world\r\n"

  describe "array parser" $ do
    it "parses an empty array" $ do
      parseOnly (arrayParser bulkStringParser) "*0\r\n" `shouldBe` Right []

    it "parses a homogeneous array" $ do
      parseOnly (arrayParser bulkStringParser) "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n" `shouldBe` Right ["hello", "world"]

  describe "array builder" $ do
    it "produces an empty array" $ do
      Builder.toLazyByteString (arrayBuilder []) `shouldBe` "*0\r\n"

    it "produces output the array parser can parse" $ property $ \(arbitraryByteStrings :: [ArbitraryByteString]) ->
      let bytestrings = map unArbitraryByteString arbitraryByteStrings
          bytestringBuilders = map bulkStringBuilder bytestrings
          toStrictByteString = ByteString.toStrict . Builder.toLazyByteString
      in
        parseOnly
          (arrayParser bulkStringParser)
          (toStrictByteString (arrayBuilder $ fromList bytestringBuilders))
        `shouldBe` Right bytestrings
