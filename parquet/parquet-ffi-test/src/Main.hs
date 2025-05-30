{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}


module Main where

import ParquetFFI
import qualified Data.ByteString.Char8 as BSC
import Foreign.C.String (withCString, newCString)
import Data.Word (Word8)
import Foreign.Ptr (Ptr, plusPtr, castPtr)
import qualified Data.ByteString as BS
import Foreign.C.Types
import Data.Int
import Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Combinators as CC
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Internal as BSL
import qualified Data.Aeson as AE
import Data.Bool
import qualified Data.Aeson as AE
import Data.Maybe
import qualified Data.Aeson.KeyMap as KM
import Debug.Trace
-- -- Helper: takes a String and gives you a Ptr Word8
-- withCStringAsWord8 :: String -> IO (Ptr Word8)
-- withCStringAsWord8 str =
--   do
--     c_str <- newCString str
--     pure $ (castPtr c_str :: Ptr Word8)

main :: IO ()
main =
  do
    let
      -- dummy = ["abc", "456", 1, 1, 1, True, "2025-02-05T20:00:00.01Z"] :: [AE.Value]
      -- dummy = ["abc", "456", 1, 1, 1, True, "2025-02-05T20:00:00.01Z"] :: [AE.Value]

    schema_cnts <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/schema.json"
    schema <- withCStringAsWord8 $ BSC.unpack schema_cnts
    schema_len <- pure $ fromInteger $ fromIntegral $ BS.length schema_cnts

    batch_cnts <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row.json"
    batch <- withCStringAsWord8 $ BSC.unpack batch_cnts
    batch_len <- pure $ fromInteger $ fromIntegral $ BS.length batch_cnts

    -- row1_cnts <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row1.json"
    -- row1 <- withCStringAsWord8 $ BSC.unpack batch_cnts
    -- row1_len <- pure $ fromInteger $ fromIntegral $ BS.length batch_cnts
    -- row2_cnts <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row2.json"
    -- row2 <- withCStringAsWord8 $ BSC.unpack batch_cnts
    -- row2_len <- pure $ fromInteger $ fromIntegral $ BS.length batch_cnts
    -- row3_cnts <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row3.json"
    -- row3 <- withCStringAsWord8 $ BSC.unpack batch_cnts
    -- row3_len <- pure $ fromInteger $ fromIntegral $ BS.length batch_cnts
    props_cnts <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/props.json"
    props <- withCStringAsWord8 $ BSC.unpack props_cnts
    props_len <- pure $ fromInteger $ fromIntegral $ BS.length props_cnts
    filePath <- withCStringAsWord8 "./demo4.parquet"

    row1 <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row1.json"
    let
      mdummy = AE.decodeStrict row1 :: Maybe AE.Object
      dummy = fromMaybe KM.empty mdummy
      -- values = map (\key -> KM.)
    ps <- new schema schema_len filePath 15 props props_len
    print dummy
    write ps $ traceShowId $ snd <$> KM.toAscList dummy

    flushRowGroup ps
    row2 <- BS.readFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row2.json"
    let
      mdummy = AE.decodeStrict row2 :: Maybe AE.Object
      dummy = fromMaybe KM.empty mdummy
      -- values = map (\key -> KM.)
    print dummy
    write ps $ traceShowId $ snd <$> KM.toAscList dummy
    -- consumeRows ps schema_cnts
    -- writeBatch ps batch (fromInteger $ fromIntegral $ BS.length batch_cnts)
    flushRowGroup ps
    closeWriter ps
    pure ()
    -- consumeRows



-- consumeRows :: Ptr ParquetSession -> IO ()
-- consumeRows ps = runConduitRes
--     $ sourceFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row.json"
--     .| decodeUtf8C
--     .| CC.linesUnbounded
--     .| mapM_C (\x -> liftIO $ do
--                   row <- withCStringAsWord8 $ T.unpack x
--                   row_len <- pure $ fromInteger $ fromIntegral $ T.length x
--                   write ps row row_len)
