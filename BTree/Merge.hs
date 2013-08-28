{-# LANGUAGE TemplateHaskell, BangPatterns, GeneralizedNewtypeDeriving #-}

module BTree.Merge (mergeTrees) where

import Prelude hiding (sum)
import Control.Applicative
import Data.Function (on)
import Data.List (sortBy)
import Data.Either (rights)
import Data.Foldable
import Control.Monad.State hiding (forM_)
import qualified Data.ByteString.Lazy as LBS
import Data.Binary       
import Control.Lens
import Pipes

import BTree.Types
import BTree.Builder
import BTree.Walk

mergeStreams :: (Monad m, Functor m)
             => (a -> a -> Ordering) -> [Producer a m ()] -> Producer a m ()
mergeStreams compare producers = do
    xs <- lift $ rights <$> mapM Pipes.next producers
    go xs
  where --go :: (Monad m, Functor m) => [(a, Producer a m ())] -> Producer a m ()
        go [] = return ()
        go xs = do let (a,producer):xs' = sortBy (compare `on` fst) xs
                   yield a
                   x' <- lift $ next producer
                   go $ either (const xs') (:xs') x'

combine :: (Monad m)
        => (a -> a -> Bool)    -- ^ equality test
        -> (a -> a -> a)       -- ^ combine operation
        -> Producer a m r -> Producer a m r
combine eq append producer = lift (next producer) >>= either return (uncurry go)
  where go a producer' = do
          n <- lift $ next producer'
          case n of
            Left r                 -> yield a >> return r
            Right (a', producer'')
              | a `eq` a'          -> go (a `append` a') producer''
              | otherwise          -> yield a >> go a' producer''
    
mergeCombine :: (Monad m, Functor m)
             => (a -> a -> Ordering) -> (a -> a -> a)
             -> [Producer a m ()] -> Producer a m ()
mergeCombine compare append producers =
    combine (\a b->compare a b == EQ) append
    $ mergeStreams compare producers 

-- | Merge two trees
mergeTrees :: (Binary k, Binary e)
           => (k -> k -> Ordering)   -- ^ ordering on keys
           -> (e -> e -> e)          -- ^ merge operation on elements
           -> Order                  -- ^ order of merged tree
           -> FilePath               -- ^ name of output file
           -> [LookupTree k e]       -- ^ trees to merge
           -> IO ()
mergeTrees compare append destOrder destFile trees = do
    let producers = map (void . walkLeaves) trees
        size = sum $ map (\hdr->hdr ^. ltHeader . btSize) trees
    fromOrderedToFile destOrder size destFile $
      mergeCombine (compare `on` key) doAppend producers
  where doAppend (BLeaf k e) (BLeaf _ e') = BLeaf k $ append e e'
        key (BLeaf k _) = k
