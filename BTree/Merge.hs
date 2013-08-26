{-# LANGUAGE TemplateHaskell, BangPatterns, GeneralizedNewtypeDeriving #-}

module BTree.Merge (merge) where

import Control.Applicative
import Data.Function (on)
import Data.List (sortBy)
import Data.Either (rights)
import Data.Maybe (fromMaybe)
import Data.Foldable
import Control.Monad.State hiding (forM_)
import Control.Monad.Trans.Class
import qualified Data.Map.Strict as M
import qualified Data.IntMap.Strict as IM
import qualified Data.Vector as V
import qualified Data.ByteString.Lazy as LBS
import Control.Lens
import Pipes

import BTree.Types
import BTree.Builder

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

combine :: Monad m => (a -> a -> Bool) -> (a -> a -> a) -> Pipe a a m r
combine eq append = await >>= go
  where go a = do
          a' <- await
          if a `eq` a'
            then go (a `append` a')
            else yield a >> go a'
    
merge :: [Producer (BTree k OnDisk v) m r] -> Producer (BTree k OnDisk v) m r
merge = undefined
