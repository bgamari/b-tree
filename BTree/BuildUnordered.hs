{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

module BTree.BuildUnordered
    ( fromUnorderedToFile ) where

import Control.Applicative
import Control.Monad.Trans.State
import Control.Error
import Data.Traversable (forM)

import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Set as S
import System.IO
import System.FilePath

import Pipes
import Pipes.Interleave
import qualified BTree.BinaryList as BL
import BTree.Types
import BTree.Merge
import BTree.Builder

-- | Maximum number of leaf lists to attempt to merge at once.
-- This is bounded by the maximum file handle count.
maxChunkMerge = 100

tempFilePath :: FilePath -> String -> IO FilePath
tempFilePath dir template = do
    (fname, h) <- liftIO $ openTempFile dir template
    hClose h
    return fname
    
-- | Build a B-tree into the given file.
--
-- This does not assume that the leaves are produced in order. Instead,
-- the sorting is handled internally through a simple merge sort. Chunks of
-- leaves are collected, sorted in memory, and then written to intermediate
-- trees. At the end these trees are then merged.
fromUnorderedToFile :: forall m e k r.
                       (MonadIO m, B.Binary (BLeaf k e), B.Binary k, B.Binary e, Ord k)
                    => FilePath                   -- ^ Path to scratch directory
                    -> Int                        -- ^ Maximum chunk size
                    -> Order                      -- ^ Order of tree
                    -> FilePath                   -- ^ Output file
                    -> Producer (BLeaf k e) m r   -- ^ 'Producer' of elements
                    -> EitherT String m ()
fromUnorderedToFile scratch maxChunk order fname prod = do
    bList <- lift (execStateT (fillLists prod) []) >>= merge
    size <- BL.length bList
    stream <- BL.stream bList
    lift $ fromOrderedToFile order size fname stream
  where
    fillLists :: Producer (BLeaf k e) m r -> StateT [BL.BinaryList (BLeaf k e)] m ()
    fillLists prod = do
      fname <- liftIO $ tempFilePath scratch "chunk.list"
      (leaves, rest) <- lift $ takeChunk maxChunk prod
      (bList, ()) <- lift $ BL.toBinaryList fname $ each leaves
      modify (bList:)

    merge :: [BL.BinaryList (BLeaf k e)] -> EitherT String m (BL.BinaryList (BLeaf k e))
    merge [l] = return l
    merge ls = do
      ls'' <- forM (splitChunks maxChunkMerge ls) $ \ls'->do
        fname <- liftIO $ tempFilePath scratch "merged.list"
        mergeLists fname ls'
      merge ls''

-- | Split the list into chunks of bounded size and run each through a function
splitChunks :: Int -> [a] -> [[a]]
splitChunks chunkSize xs = go xs
  where
    go [] = []
    go xs = let (prefix,suffix) = splitAt chunkSize xs
            in prefix : go suffix

throwLeft :: Monad m => m (Either String r) -> m r
throwLeft action = action >>= either error return

mergeLists :: (Ord a, B.Binary a, MonadIO m)
           => FilePath -> [BL.BinaryList a] -> EitherT String m (BL.BinaryList a)
mergeLists dest lists = do
    streams <- mapM BL.stream lists
    let prod = interleave compare (map throwLeft streams)
    (bList, ()) <- lift $ BL.toBinaryList dest prod
    return bList

-- | Take the first 'n' elements and collect them in a 'Set'. Return
-- a 'Producer' which will emit the remaining elements (or the return
-- value).
takeChunk :: forall m a r. (Monad m, Ord a)
          => Int
          -> Producer a m r
          -> m (S.Set a, Either r (Producer a m r))
takeChunk n prod = go n S.empty prod
  where
    go :: Int -> S.Set a -> Producer a m r -> m (S.Set a, Either r (Producer a m r))
    go 0 s prod = return (s, Right prod)
    go i s prod = do
      result <- next prod
      case result of
        Left r -> return (s, Left r)
        Right (a, prod') -> go (i-1) (S.insert a s) prod'
