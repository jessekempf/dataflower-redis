{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Net.Redis.ProtocolSpec (spec) where

import           Data.Attoparsec.ByteString (parseOnly)
import           Data.ByteString            (ByteString)
import qualified Data.ByteString            as ByteString
import qualified Data.ByteString.Builder    as Builder
import qualified Data.Set                   as Set
import           Data.Vector                (fromList)
import           Net.Redis.Protocol
import           Test.Hspec
import           Test.QuickCheck            (Arbitrary (arbitrary),
                                             Testable (property))

newtype ArbitraryByteString = ArbitraryByteString { unArbitraryByteString :: ByteString } deriving Show

instance Arbitrary ArbitraryByteString where
  arbitrary = ArbitraryByteString . ByteString.pack <$> arbitrary

spec :: Spec
spec = do
  describe "RESP Primitive Parser" $ do
    it "decodes simple strings" $ do
      parseOnly fromRESP "+foobar\r\n" `shouldBe` Right (RESPSimpleString "foobar")

    it "decodes simple errors" $ do
      parseOnly fromRESP "-oh noes\r\n" `shouldBe` Right (RESPSimpleError "oh noes")

    it "decodes integers" $ do
      parseOnly fromRESP ":1234567890123456789\r\n" `shouldBe` Right (RESPInteger 1234567890123456789)

    it "decodes bulk strings" $ do
      parseOnly fromRESP "$11\r\nhello world\r\n" `shouldBe` Right (RESPBulkString "hello world")

    it "decodes nulls" $ do
      parseOnly fromRESP "_\r\n" `shouldBe` Right RESPNull

    it "decodes booleans" $ do
      parseOnly fromRESP "#t\r\n" `shouldBe` Right (RESPBoolean True)
      parseOnly fromRESP "#f\r\n" `shouldBe` Right (RESPBoolean False)

    it "decodes doubles" $ do
      parseOnly fromRESP ",-1.2345E6\r\n" `shouldBe` Right (RESPDouble (-1234500))
      parseOnly fromRESP ",1.2345e6\r\n" `shouldBe` Right (RESPDouble 1234500)

    it "decodes bignums" $ do
      parseOnly fromRESP "(123\r\n" `shouldBe` Right (RESPBignum 123)

    it "decodes verbatim strings" $ do
      parseOnly fromRESP "=15\r\ntxt:Some string\r\n" `shouldBe` Right (RESPVerbatimString "txt" "Some string")

  describe "RESP Primitive Encoder" $ do
    it "produces parseable output" $ property $ \(primitive :: RESPPrimitive) -> do
      parseOnly fromRESP (ByteString.toStrict . Builder.toLazyByteString $ toRESP primitive) `shouldBe` Right primitive

  describe "RESP Value Parser" $ do
    it "decodes primitives" $ do
      parseOnly fromRESP "+foobar\r\n" `shouldBe` Right (RESPPrimitive' (RESPSimpleString "foobar"))

    it "decodes arrays" $ do
      parseOnly fromRESP "*0\r\n" `shouldBe` Right (RESPArray [])

      parseOnly fromRESP "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
        `shouldBe` Right (RESPArray (map (RESPPrimitive' . RESPBulkString) ["hello", "world"]))

      parseOnly fromRESP "*3\r\n:1\r\n:2\r\n:3\r\n"
        `shouldBe` Right (RESPArray (map (RESPPrimitive' . RESPInteger) [1, 2, 3]))

    it "decodes maps" $ do
      parseOnly fromRESP "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"
        `shouldBe` Right (RESPMap [
          (RESPPrimitive' $ RESPSimpleString "first",  RESPPrimitive' $ RESPInteger 1),
          (RESPPrimitive' $ RESPSimpleString "second", RESPPrimitive' $  RESPInteger 2)
        ])

    it "decodes complex data" $ do
      parseOnly fromRESP "*2\r\n$6\r\nfield1\r\n$5\r\nHello\r\n"
        `shouldBe` Right (RESPArray [
          RESPPrimitive' $ RESPBulkString "field1",
          RESPPrimitive' $ RESPBulkString "Hello"
        ])

    it "decodes sets" $ do
      parseOnly fromRESP "~2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
        `shouldBe` Right (RESPSet $ Set.fromList $ map (RESPPrimitive' . RESPBulkString) ["hello", "world"])

  describe "RESP Value Encoder" $ do
    it "produces parseable output" $ property $ \(value :: RESPValue) -> do
      let str = ByteString.toStrict . Builder.toLazyByteString $ toRESP value
      parseOnly fromRESP str `shouldBe` Right value