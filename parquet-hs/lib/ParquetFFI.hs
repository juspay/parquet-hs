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
import Data.Foldable
import Foreign.C.String (newCString)
import Data.ByteString.Unsafe
import qualified Data.ByteString as BS
import qualified Data.Aeson as AE
import qualified Data.ByteString.Lazy as LBS

data ParquetSession

foreign import ccall "new"
  new :: Ptr Word8 -> CInt -> Ptr Word8 -> CInt -> Ptr Word8 -> CInt -> IO (Ptr ParquetSession)

foreign import ccall "write_batch"
  writeBatchInternal :: Ptr ParquetSession -> Ptr Word8 -> CInt -> IO ()

foreign import ccall "flush_row_group"
  flushRowGroup :: Ptr ParquetSession -> IO ()

foreign import ccall "close_ps_writer"
  closeWriter :: Ptr ParquetSession -> IO()

withCStringAsWord8 :: String -> IO (Ptr Word8)
withCStringAsWord8 str =
  do
    c_str <- newCString str
    pure $ (castPtr c_str :: Ptr Word8)

writeBatch :: (Foldable t, AE.ToJSON a) => Ptr ParquetSession -> t (t a) -> IO ()
writeBatch ps rows = do
  let
    rows_lbs = LBS.toStrict $ AE.encode $ map toList (toList rows)
  unsafeUseAsCString
    rows_lbs
    (\rows_cs ->
        writeBatchInternal ps (castPtr rows_cs) (fromIntegral $ BS.length rows_lbs))

write :: (Foldable t, AE.ToJSON a, Show a, Show (t a)) => Ptr ParquetSession -> t a -> IO ()
write ps row = do
  let
    row_lbs = LBS.toStrict $ AE.encode $ map (\e -> [e]) (toList row)
  unsafeUseAsCString
    row_lbs
    (\row_cs ->
        writeBatchInternal ps (castPtr row_cs) (fromIntegral $ BS.length row_lbs))
