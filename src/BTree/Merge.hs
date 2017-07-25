module BTree.Merge ( mergeTrees
                   , mergeLeaves
                   , sizedProducerForTree
                   ) where

import Control.Applicative
import Data.Foldable
import Control.Monad.State hiding (forM_)
import Control.Monad.Catch
import Data.Binary
import Control.Lens
import Pipes
import Pipes.Interleave
import Prelude hiding (sum)

import BTree.Types
import BTree.Builder
import BTree.Walk

-- | Merge trees' leaves taking ordered leaves from a set of producers.
--
-- Each producer must be annotated with the number of leaves it is
-- expected to produce. The size of the resulting tree will be at most
-- the sum of these sizes.
mergeLeaves :: (MonadMask m, MonadIO m, Functor m, Binary k, Binary e, Ord k)
            => (e -> e -> m e)               -- ^ merge operation on elements
            -> Order                         -- ^ order of merged tree
            -> FilePath                      -- ^ name of output file
            -> [(Size, Producer (BLeaf k e) m ())]   -- ^ producers of leaves to merge
            -> m ()
mergeLeaves append destOrder destFile producers = do
    let size = sum $ map fst producers
    fromOrderedToFile destOrder size destFile $
      mergeM doAppend (map snd producers)
  where
    doAppend (BLeaf k e) (BLeaf _ e') = BLeaf k <$> append e e'
{-# INLINE mergeLeaves #-}

-- | Merge several 'LookupTrees'
--
-- This is a convenience function for merging several trees already on
-- disk. For a more flexible interface, see 'mergeLeaves'.
mergeTrees :: (MonadMask m, MonadIO m, Functor m, Binary k, Binary e, Ord k)
           => (e -> e -> m e)        -- ^ merge operation on elements
           -> Order                  -- ^ order of merged tree
           -> FilePath               -- ^ name of output file
           -> [LookupTree k e]       -- ^ trees to merge
           -> m ()
mergeTrees append destOrder destFile trees = do
    mergeLeaves append destOrder destFile
    $ map sizedProducerForTree trees
{-# INLINE mergeTrees #-}

-- | Get a sized 'Producer' suitable for 'mergeLeaves' from a 'LookupTree'
sizedProducerForTree :: (Monad m, Binary k, Binary e)
                     => LookupTree k e   -- ^ a tree
                     -> (Size, Producer (BLeaf k e) m ())
                                         -- ^ a sized 'Producer' suitable for passing
                                         -- to 'mergeLeaves'
sizedProducerForTree lt = (lt ^. ltHeader . btSize, void $ walkLeaves lt)
