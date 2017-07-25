{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BTree.BinaryList
    ( BinaryList
      -- * Construction
    , toBinaryList
      -- * Fetching contents
    , open
    , stream
      -- * Other queries
    , length
    , filePath
    ) where

import Control.Applicative
import Control.Monad.Trans.Class
import Control.Monad.Catch
import Control.Error
import Data.Word
import System.IO
import Prelude hiding (length)

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import qualified Data.Binary.Builder as BB
import Pipes

import BTree.BinaryFile

-- | A file containing a finite list of binary encoded items
newtype BinaryList a = BinList FilePath
                     deriving (Show)

-- | Get the path to the @BinaryList@ file
filePath :: BinaryList a -> FilePath
filePath (BinList f) = f

data Header = Header { hdrLength :: Word64 }
            deriving (Show)

instance B.Binary Header where
    get = Header <$> B.getWord64le
    put (Header l) = B.putWord64le l

-- | Encode the items of the given producer
toBinaryList :: forall m a r. (MonadMask m, MonadIO m, B.Binary a)
             => FilePath -> Producer a m r -> m (BinaryList a, r)
toBinaryList fname producer = do
    writeWithHeader fname (go 0 producer BB.empty)
  where
    go :: Int -> Producer a m r -> BB.Builder
       -> Producer LBS.ByteString m (Header, (BinaryList a, r))
    go !n prod accum = do
        result <- lift $ next prod
        case result of
          Left r -> do
            let hdr = Header (fromIntegral n)
            yield $ BB.toLazyByteString accum
            return (hdr, (BinList fname, r))
          Right (a, prod')
            | n `mod` 100 == 0 -> do
              yield $ BB.toLazyByteString accum
              go (n+1) prod' (B.execPut $ B.put a)
            | otherwise ->
              go (n+1) prod' (accum `BB.append` B.execPut (B.put a))
{-# INLINE toBinaryList #-}

-- | Open a 'BinaryList' file.
--
-- TODO: Sanity checking at open time.
open :: FilePath -> BinaryList a
open = BinList

withHeader :: (MonadMask m, MonadIO m)
           => BinaryList a -> (Header -> Handle -> m b) -> ExceptT String m b
withHeader (BinList fname) action = readWithHeader fname action

length :: (MonadMask m, MonadIO m)
       => BinaryList a -> ExceptT String m Word64
length bl = withHeader bl $ \hdr _ -> return $ hdrLength hdr

-- | Stream the items out of a @BinaryList@
stream :: forall m a. (B.Binary a, MonadMask m, MonadIO m)
       => BinaryList a -> ExceptT String m (Producer a m (Either String ()))
stream bl = withHeader bl readContents
  where
    readContents :: Header -> Handle -> m (Producer a m (Either String ()))
    readContents hdr h = return $ liftIO (LBS.hGetContents h) >>= go (hdrLength hdr)

    go :: Word64 -> LBS.ByteString -> Producer a m (Either String ())
    go 0  _  = return $ Right ()
    go !n bs =
      case B.decodeOrFail bs of
        Left (_, _, e)    -> return $ Left e
        Right (bs', _, a) -> yield a >> go (n-1) bs'
{-# INLINE stream #-}
