{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

module BTree.BuildUnordered
    ( fromUnorderedToFile
    , fromUnorderedToList
    ) where

import Control.Monad.Trans.State
import Control.Monad.Catch
import Control.Error
import Data.Traversable (forM)

import qualified Data.Binary as B
import qualified Data.Set as S
import System.IO
import System.Directory (removeFile)

import Pipes
import Pipes.Interleave
import qualified BTree.BinaryList as BL
import BTree.Types
import BTree.Builder

-- | Maximum number of leaf lists to attempt to merge at once.
-- This is bounded by the maximum file handle count.
maxChunkMerge :: Int
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
                       (MonadMask m, MonadIO m,
                        B.Binary (BLeaf k e), B.Binary k, B.Binary e, Ord k)
                    => FilePath                   -- ^ Path to scratch directory
                    -> Int                        -- ^ Maximum chunk size
                    -> Order                      -- ^ Order of tree
                    -> FilePath                   -- ^ Output file
                    -> Producer (BLeaf k e) m r   -- ^ 'Producer' of elements
                    -> ExceptT String m ()
fromUnorderedToFile scratch maxChunk order dest producer = {-# SCC fromUnorderedToFile #-} do
    bList <- fromUnorderedToList scratch maxChunk producer
    size <- BL.length bList
    stream <- {-# SCC stream #-} BL.stream bList
    lift $ {-# SCC buildTree #-} fromOrderedToFile order size dest stream
    liftIO $ removeFile $ BL.filePath bList
{-# INLINE fromUnorderedToFile #-}

fromUnorderedToList :: forall m a r.
                       (MonadMask m, MonadIO m, B.Binary a, Ord a)
                    => FilePath                   -- ^ Path to scratch directory
                    -> Int                        -- ^ Maximum chunk size
                    -> Producer a m r             -- ^ 'Producer' of elements
                    -> ExceptT String m (BL.BinaryList a)
fromUnorderedToList scratch maxChunk producer = do
    lift (execStateT (fillLists producer) []) >>= {-# SCC goMerge #-} goMerge
  where
    fillLists :: Producer a m r -> StateT [BL.BinaryList a] m r
    fillLists prod = {-# SCC fillLists #-} do
      fname <- liftIO $ tempFilePath scratch "chunk.list"
      (leaves, rest) <- lift $ takeChunk maxChunk prod
      (bList, ()) <- lift $ BL.toBinaryList fname $ each $ S.toAscList leaves
      modify (bList:)
      case rest of
        Left r         -> return r
        Right nextProd -> fillLists nextProd

    goMerge :: [BL.BinaryList a] -> ExceptT String m (BL.BinaryList a)
    goMerge [l] = return l
    goMerge ls = do
      ls'' <- forM (splitChunks maxChunkMerge ls) $ \ls'->do
        fname <- liftIO $ tempFilePath scratch "merged.list"
        list <- mergeLists fname ls'
        liftIO $ mapM_ (removeFile . BL.filePath) ls'
        return list
      goMerge ls''
{-# INLINE fromUnorderedToList #-}

-- | Split the list into chunks of bounded size and run each through a function
splitChunks :: Int -> [a] -> [[a]]
splitChunks chunkSize = go
  where
    go [] = []
    go xs = let (prefix,suffix) = splitAt chunkSize xs
            in prefix : go suffix
{-# INLINE splitChunks #-}

throwLeft :: Monad m => m (Either String r) -> m r
throwLeft action = action >>= either error return

mergeLists :: (B.Binary a, Ord a, MonadMask m, MonadIO m)
           => FilePath
           -> [BL.BinaryList a]
           -> ExceptT String m (BL.BinaryList a)
mergeLists dest lists = do
    streams <- mapM BL.stream lists
    let prod = interleave (map throwLeft streams)
    (bList, ()) <- lift $ BL.toBinaryList dest prod
    return bList
{-# INLINE mergeLists #-}

-- | Take the first 'n' elements and collect them in a 'Set'. Return
-- a 'Producer' which will emit the remaining elements (or the return
-- value).
takeChunk :: forall m a r. (Monad m, Ord a)
          => Int
          -> Producer a m r
          -> m (S.Set a, Either r (Producer a m r))
takeChunk n = go n S.empty
  where
    go :: Int -> S.Set a -> Producer a m r -> m (S.Set a, Either r (Producer a m r))
    go 0 s prod = return (s, Right prod)
    go i s prod = do
      result <- next prod
      case result of
        Left r -> return (s, Left r)
        Right (a, prod') -> go (i-1) (S.insert a s) prod'
{-# INLINE takeChunk #-}
