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
import Foreign.Ptr (Ptr, castPtr)
import Foreign.C.Types
import Foreign.C.String (newCString)
import Data.Int
import Data.Foldable
import qualified Data.Aeson as AE
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy.UTF8 as LBSU

data ParquetSession

foreign import ccall "new"
  new :: Ptr Word8 -> CInt -> Ptr Word8 -> CInt -> Ptr Word8 -> CInt -> IO (Ptr ParquetSession)

foreign import ccall "write_batch"
  writeBatchInternal :: Ptr ParquetSession -> Ptr Word8 -> CInt -> IO ()

foreign import ccall "flush_row_group"
  flushRowGroup :: Ptr ParquetSession -> IO ()

foreign import ccall "close_writer"
  closeWriter :: Ptr ParquetSession -> IO()

withCStringAsWord8 :: String -> IO (Ptr Word8)
withCStringAsWord8 str =
  do
    c_str <- newCString str
    pure $ (castPtr c_str :: Ptr Word8)

intToCInt :: Int64 -> CInt
intToCInt n = fromInteger $ fromIntegral n

writeBatch :: (Foldable t, AE.ToJSON a) => Ptr ParquetSession -> t (t a) -> IO ()
writeBatch ps rows = do
  let
    rows_lbs = AE.encode $ map toList (toList rows)
  rows_w8 <- withCStringAsWord8 $ LBSU.toString rows_lbs
  writeBatchInternal ps rows_w8 (intToCInt $ LBS.length rows_lbs)

write :: (Foldable t, AE.ToJSON a, Show a, Show (t a)) => Ptr ParquetSession -> t a -> IO ()
write ps row = do
  let
    row_lbs = AE.encode $ map (\e -> [e]) (toList row)
  row_w8 <- withCStringAsWord8 $ LBSU.toString row_lbs
  writeBatchInternal ps row_w8 (intToCInt $ LBS.length row_lbs)
