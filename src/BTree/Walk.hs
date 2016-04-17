{-# LANGUAGE BangPatterns #-}

module BTree.Walk ( walkLeaves
                  , walkNodes
                  , walkNodesWithOffset
                  ) where

import BTree.Types
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import Pipes
import qualified Pipes.Prelude as PP
import Data.Binary
import Data.Binary.Get (runGetOrFail)
import Control.Lens

-- If we only look at leaves keys will increase monotonically as we
-- progress through the file.

filterLeaves :: Monad m => Pipe (BTree k OnDisk v) (BLeaf k v) m r
filterLeaves = PP.mapFoldable getLeaf
  where
    getLeaf (Leaf leaf) = Just leaf
    getLeaf _           = Nothing
{-# INLINE filterLeaves #-}

-- | Iterate over the leaves of the given tree in ascending key order.
walkLeaves :: (Binary k, Binary v, Monad m)
           => LookupTree k v
           -> Producer (BLeaf k v) m (LBS.ByteString, Maybe String)
walkLeaves b = walkNodes b >-> filterLeaves
{-# INLINE walkLeaves #-}

-- | Iterate over the nodes and leaves of the given tree. These aren't
-- necessarily sorted.
walkNodes :: (Binary k, Binary v, Monad m)
          => LookupTree k v
          -> Producer (BTree k OnDisk v) m (LBS.ByteString, Maybe String)
walkNodes b = walkNodesWithOffset b >-> PP.map snd
{-# INLINE walkNodes #-}

walkNodesWithOffset :: (Binary k, Binary v, Monad m)
                    => LookupTree k v
                    -> Producer (Offset, BTree k OnDisk v) m (LBS.ByteString, Maybe String)
walkNodesWithOffset = go 0 . {-# SCC "buffer" #-}view ltData
  where go !offset bs =
            case runGetOrFail get (LBS.fromStrict bs) of
              Left (rest,_,err)  -> return (rest, Just err)
              Right (_,o,a)      -> do
                yield (offset, a)
                let rest = BS.drop (fromIntegral o) bs
                if BS.null rest
                  then return (LBS.fromStrict rest, Nothing)
                  else go (offset+o) rest
{-# INLINE walkNodesWithOffset #-}
