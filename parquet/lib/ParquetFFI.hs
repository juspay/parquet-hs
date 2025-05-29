{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances#-}
{-# LANGUAGE TemplateHaskell #-}


module ParquetFFI where

import Data.Word (Word8)
import Foreign.Ptr (Ptr, plusPtr, castPtr)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy.Internal as LBS
import qualified Data.ByteString.Lazy as LBS
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.C.Types
import Foreign.C.String (withCString, newCString)
import qualified Data.HashMap.Internal as HM
import qualified Data.Conduit.Combinators as CC
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Conduit
import Data.Aeson.Types (Value)
import qualified Data.Aeson.KeyMap as KM
import qualified Data.Aeson as AE
import Data.Maybe
import Debug.Trace
import qualified Data.Map.Ordered as MAPO
import qualified Data.Aeson.Micro as AM
import Data.Aeson.Types (FromJSON, parseJSON)
import qualified Data.HashMap.Strict.InsOrd as OHM
import Data.ByteString.Lazy.UTF8 (toString)
import Data.Int

-- import Data.Aeson.TH

-- import Data.Aeson

data ParquetSession

-- instance FromJSON v => FromJSON (MAPO.OMap T.T/ext v) where
  -- parseJSON = AE.withObject "Map Text v" $ mapM parseJSON

-- instance Generic (MAPO.OMap a b)
-- instance AE.GFromJSON AE.Zero (MAPO.OMap a b)
-- instance FromJSON (MAPO.OMap a b)

-- deriving instance FromJSON (MAPO.OMap a b)
-- deriving instance Generic (MAPO.OMap a b)

-- $(deriveFromJSON defaultOptions ''MAPO.OMap )

foreign import ccall "new"
  new :: Ptr Word8 -> CInt -> Ptr Word8 -> CInt -> Ptr Word8 -> CInt -> IO (Ptr ParquetSession)

foreign import ccall "write_batch"
  writeBatch :: Ptr ParquetSession -> Ptr Word8 -> CInt -> IO ()

foreign import ccall "flush_row_group"
  flushRowGroup :: Ptr ParquetSession -> IO ()

foreign import ccall "close_writer"
  closeWriter :: Ptr ParquetSession -> IO()

-- Helper: takes a String and gives you a Ptr Word8
withCStringAsWord8 :: String -> IO (Ptr Word8)
withCStringAsWord8 str =
  do
    c_str <- newCString str
    pure $ (castPtr c_str :: Ptr Word8)

intToCInt :: Int64 -> CInt
intToCInt n = fromInteger $ fromIntegral n

extractRowData :: BS.ByteString -> BS.ByteString -> [Value]
extractRowData js schema = do
  let
    js_km = AE.decodeStrict js :: Maybe AE.Object
    schema_km = AE.decodeStrict schema :: Maybe AE.Object
  catMaybes $ maybe
    []
    (\(sc, js) -> map (\key -> KM.lookup key js) $ fst <$> KM.toList sc)
    ((,) <$> schema_km <*> js_km)


consumeRows :: Ptr ParquetSession -> BS.ByteString -> IO ()
consumeRows ps schema = do
  let hm = HM.empty
  rows <- runConduitRes
    $ CC.sourceFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/flat_logs.json"
    .| decodeUtf8C
    .| CC.linesUnbounded
    .| CC.map (\js -> do
                  extractRowData (TE.encodeUtf8 js) schema)
                  -- print row
                  -- print schema
                  -- row <- withCStringAsWord8 $ T.unpack x
                  -- row_len <- pure $ fromInteger $ fromIntegral $ T.length x
                  -- write ps row row_len
                  -- pure ())
    .| CC.sinkList

  let
    rows_str = AE.encode rows
  rows_w8 <- withCStringAsWord8 $ toString $ AE.encode rows
  writeBatch ps rows_w8 (intToCInt $ LBS.length rows_str)

-- .| CC.print

-- decode :: LBS.ByteString -> Maybe a
-- decode bs = unResult (toResultValue (lbsToTokens bs)) (\_ -> Nothing) $ \v bs' -> case A.ifromJSON v of
--     A.ISuccess x
--         | lbsSpace bs' -> Just x
--         | otherwise    -> Nothing
--     A.IError _ _       -> Nothing

-- form_row :: ByteString

-- consumeRows :: Ptr ParquetSession -> IO ()
-- consumeRows ps = runConduitRes
--     $ sourceFile "/Users/fazal.mohammad/Work/parquet-ffi/parquet-rs/src/row.json"
--     .| decodeUtf8C
--     .| CC.linesUnbounded
--     .| mapM_C (\x -> liftIO $ do
--                   row <- withCStringAsWord8 $ T.unpack x
--                   row_len <- pure $ fromInteger $ fromIntegral $ T.length x
--                   write ps row row_len)


-- TODO: create list of list with inner list belonging to same column
-- or list of list with inner list as a row and later transpose?
-- anyways our input will be a streaming json
-- use conduit
-- when stream ends flush ><
-- dont need to flush user can flush whenever user wants
-- this function can be called multiple times
--
