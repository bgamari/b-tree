{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BTree.BinaryList
    ( BinaryList
    , toBinaryList
    , stream
    ) where

import Control.Monad.Trans.Class
import Control.Monad.Trans.Either
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

-- | Encode the items of the given producer
toBinaryList :: forall m a. (MonadIO m, B.Binary a)
             => FilePath -> Producer a m () -> m (BinaryList a)
toBinaryList fname prod = do
    writeWithHeader fname (go 0 prod)
  where
    go :: Int -> Producer a m r -> Producer LBS.ByteString m (Int, BinaryList a)
    go !n prod = do
        result <- lift $ next prod
        case result of
          Left r -> return (n, BinList fname)
          Right (a, prod') -> do
            yield (B.encode a)
            go (n+1) prod'

-- | Stream the items out of a @BinaryList@
stream :: forall m a. (B.Binary a, MonadIO m)
       => BinaryList a -> EitherT String m (Producer a m (Either String ()))
stream (BinList fname) = readWithHeader fname readContents
  where
    readContents :: Int -> Handle -> m (Producer a m (Either String ()))
    readContents len h = return $ liftIO (LBS.hGetContents h) >>= go len

    go :: Int -> LBS.ByteString -> Producer a m (Either String ())
    go 0  _  = return $ Right ()
    go !n bs =
      case B.decodeOrFail bs of
        Left (_, _, err)  -> return $ Left err
        Right (bs', _, a) -> yield a >> go (n-1) bs'

test :: IO ()
test = do
    lst <- toBinaryList "hi.list" $ each [0..100000::Int]
    res <- runEitherT $ stream lst
    case res of
      Left err -> print err
      Right prod -> runEffect (for prod $ liftIO . print) >>= print
