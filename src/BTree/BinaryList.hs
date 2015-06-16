{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BTree.BinaryList
    ( BinaryList
      -- * Construction
    , toBinaryList
      -- * Fetching contents
    , stream
      -- * Other queries
    , length
    , filePath
    ) where

import Prelude hiding (length)
import Control.Applicative
import Control.Monad.Trans.Class
import Control.Error
import Data.Word
import System.IO

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
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
toBinaryList :: forall m a r. (MonadIO m, B.Binary a)
             => FilePath -> Producer a m r -> m (BinaryList a, r)
toBinaryList fname producer = do
    writeWithHeader fname (go 0 producer)
  where
    go :: Int -> Producer a m r
       -> Producer LBS.ByteString m (Header, (BinaryList a, r))
    go !n prod = do
        result <- lift $ next prod
        case result of
          Left r ->
            let hdr = Header (fromIntegral n)
            in return (hdr, (BinList fname, r))
          Right (a, prod') -> do
            yield (B.encode a)
            go (n+1) prod'
{-# INLINE toBinaryList #-}

withHeader :: MonadIO m
           => BinaryList a -> (Header -> Handle -> m b) -> ExceptT String m b
withHeader (BinList fname) action = readWithHeader fname action

length :: MonadIO m => BinaryList a -> ExceptT String m Word64
length bl = withHeader bl $ \hdr _ -> return $ hdrLength hdr

-- | Stream the items out of a @BinaryList@
stream :: forall m a. (B.Binary a, MonadIO m)
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
