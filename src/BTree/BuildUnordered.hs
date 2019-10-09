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
import qualified Data.Map.Strict as M
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


type Resource = (FilePath,Handle)

cleanResource :: Resource -> IO ()
cleanResource (fname,fhandle) = hClose fhandle >> removeFile fname

-- | Build a B-tree into the given file.
--
-- This does not assume that the leaves are produced in order. Instead,
-- the sorting is handled internally through a simple merge sort. Chunks of
-- leaves are collected, sorted in memory, and then written to intermediate
-- trees. At the end these trees are then merged.
fromUnorderedToFile :: forall m k v r.
                       (MonadMask m, MonadIO m, B.Binary k, B.Binary v, Ord k)
                    => FilePath                   -- ^ Path to scratch directory
                    -> Int                        -- ^ Maximum chunk size
                    -> Order                      -- ^ Order of tree
                    -> FilePath                   -- ^ Output file
                    -> (v -> v -> v)              -- ^ merge functions
                    -> Producer (BLeaf k v) m r   -- ^ 'Producer' of elements
                    -> ExceptT String m ()
fromUnorderedToFile scratch maxChunk order dest mergeV producer = {-# SCC fromUnorderedToFile #-} do
    (bList,resource) <- fromUnorderedToList scratch maxChunk mergeV producer
    size             <- BL.length bList
    stream           <- {-# SCC stream #-} BL.stream bList
    lift $ {-# SCC buildTree #-} fromOrderedToFile order size dest stream
    liftIO $ cleanResource resource

{-# INLINE fromUnorderedToFile #-}

fromUnorderedToList :: forall m k v r.
                       (MonadMask m, MonadIO m, B.Binary k, B.Binary v, Ord k)
                    => FilePath                   -- ^ Path to scratch directory
                    -> Int                        -- ^ Maximum chunk size
                    -> (v -> v -> v)              -- ^ merge functions
                    -> Producer (BLeaf k v) m r   -- ^ 'Producer' of elements
                    -> ExceptT String m (BL.BinaryList (BLeaf k v),Resource)
fromUnorderedToList scratch maxChunk mergeV producer = lift (execStateT (fillLists producer) []) >>= {-# SCC goMerge #-} goMerge
  where
    fillLists :: Producer (BLeaf k v) m r -> StateT [(BL.BinaryList (BLeaf k v),Resource)] m r
    fillLists prod = {-# SCC fillLists #-} do
      resource@(_,fhandle) <- liftIO $ openTempFile scratch "chunk.list"
      (leaves, rest)  <- lift $ takeChunk maxChunk mergeV prod
      (bList, ())     <- lift $ BL.toBinaryList fhandle $ each $ [ BLeaf k v | (k,v) <- M.toAscList leaves]
      modify ((bList,resource):)
      case rest of
        Left r         -> return r
        Right nextProd -> fillLists nextProd

    goMerge :: [(BL.BinaryList (BLeaf k v),Resource)] -> ExceptT String m (BL.BinaryList (BLeaf k v), Resource)
    goMerge [l] = return l
    goMerge ls = do
      ls'' <- forM (splitChunks maxChunkMerge ls) $ \ls'->do
        resource@(_, fhandle) <- liftIO $ openTempFile scratch "merged.list"
        let (chunk,chunkResource) = unzip ls'
        list <- mergeLists fhandle mergeV chunk
        liftIO $ mapM_ cleanResource chunkResource
        return (list,resource)
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

mergeLists :: (B.Binary k, Ord k, B.Binary v, MonadMask m, MonadIO m)
           => Handle
           -> (v -> v -> v)
           -> [BL.BinaryList (BLeaf k v)]
           -> ExceptT String m (BL.BinaryList (BLeaf k v))
mergeLists dest mergeV lists = do
    streams <- mapM BL.stream lists
    
    let prod = leafMerger mergeV $ interleave (map throwLeft streams)

    (bList, ()) <- lift $ BL.toBinaryList dest prod
    return bList
{-# INLINE mergeLists #-}

-- | Take the first 'n' elements and collect them in a 'Set'. Return
-- a 'Producer' which will emit the remaining elements (or the return
-- value).
takeChunk :: forall m k v r. (Monad m, Ord k)
          => Int
          -> (v -> v -> v)
          -> Producer (BLeaf k v) m r
          -> m (M.Map k v, Either r (Producer (BLeaf k v) m r))
takeChunk n mergeV = go n M.empty
  where
    go :: Int -> M.Map k v -> Producer (BLeaf k v) m r -> m (M.Map k v, Either r (Producer (BLeaf k v) m r))
    go 0 s prod = return (s, Right prod)
    go i s prod = do
      result <- next prod
      case result of
        Left r -> return (s, Left r)
        Right (BLeaf k v, prod') -> go (i-1) (M.insertWith mergeV k v s) prod'
{-# INLINE takeChunk #-}




-- | 'combine' with monadic side-effects in the combine operation.
leafMerger :: (Monad m, Eq k)
           => (v -> v -> v)     -- ^ combine operation
           -> Producer (BLeaf k v) m r -> Producer (BLeaf k v) m r
leafMerger mergeV producer = lift (next producer) >>= either return (uncurry go)
  where go a@(BLeaf k v) producer' = do
          n <- lift $ next producer'
          case n of
            Left r                 -> yield a >> return r
            Right (a'@(BLeaf k' v'), producer'')
              | k == k'            -> go (BLeaf k' $ mergeV v v') producer''
              | otherwise          -> yield a >> go a' producer''
{-# INLINABLE leafMerger #-}


