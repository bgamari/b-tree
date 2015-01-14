module BTree.BinaryFile
    ( writeWithHeader
    , readWithHeader
    ) where

import Control.Monad (when)
import Control.Error
import Control.Monad.Trans.Class
import Control.Applicative
import Data.Word
import System.IO

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import Pipes

-- | This module provides helpers for emitting and reading binary files with
-- a trailing "header".

-- | An internal data structure placed at the very end of the file which
-- describes the header and provides a magic number for sanity checking.
data Epilogue = Epilogue { magic :: Word64
                         , headerLen :: Word64
                         }
              deriving (Show)

epiLength :: Integer
epiLength = 16

magicNumber :: Word64
magicNumber = 0xdeadbeef

instance B.Binary Epilogue where
    get = Epilogue <$> B.getWord64le <*> B.getWord64le
    put (Epilogue m l) = B.putWord64le m >> B.putWord64le l

-- | Write the produced bytestrings to the file followed by the
-- returned header
writeWithHeader :: (MonadIO m, B.Binary hdr)
                => FilePath
                -> Producer LBS.ByteString m (hdr, r)
                -> m r
writeWithHeader fname prod = do
    h <- liftIO $ openFile fname WriteMode
    (hdr, r) <- runEffect $ for prod (liftIO . LBS.hPut h)
    let encoded = B.encode hdr
    liftIO $ LBS.hPut h encoded
    let epi = Epilogue { magic = magicNumber
                       , headerLen = fromIntegral $ LBS.length encoded }
    liftIO $ LBS.hPut h (B.encode epi)
    liftIO $ hClose h
    return r

annotate :: Monad m => String -> EitherT String m a -> EitherT String m a
annotate ann = fmapLT ((ann++": ")++)

runGetT :: Monad m => B.Get a -> LBS.ByteString -> EitherT String m a
runGetT _get bs = do
    case B.runGetOrFail _get bs of
      Left (_, _, e)  -> left e
      Right (_, _, a) -> right a

-- | Read and verify the header from the file, then pass it along with the
-- file's handle to an action. The file handle sits at the beginning of the
-- written content when passed to the action.
readWithHeader :: (MonadIO m, B.Binary hdr)
               => FilePath
               -> (hdr -> Handle -> m a)
               -> EitherT String m a
readWithHeader fname action = do
    h <- liftIO $ openFile fname ReadMode
    -- read epilogue
    liftIO $ hSeek h SeekFromEnd (-epiLength)
    epiBytes <- liftIO (LBS.hGet h $ fromIntegral epiLength)
    epi <- annotate "Error reading epilogue" (runGetT B.get epiBytes)
    when (magic epi /= magicNumber) $
        left "BinaryFile.readWithHeader: Bad magic number"
    -- read header
    let offset = fromIntegral epiLength + fromIntegral (headerLen epi)
    liftIO $ hSeek h SeekFromEnd (negate offset)
    hdrBytes <- liftIO (LBS.hGet h $ fromIntegral $ headerLen epi)
    hdr <- annotate "Error reading header" (runGetT B.get hdrBytes)
    -- pass control to action
    liftIO $ hSeek h AbsoluteSeek 0
    lift $ action hdr h