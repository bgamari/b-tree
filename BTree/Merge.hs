{-# LANGUAGE TemplateHaskell, BangPatterns, GeneralizedNewtypeDeriving #-}

module BTree.Merge (merge) where

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

merge :: (Monad m, Functor m)
      => (a -> a -> Ordering) -> [Producer a m ()] -> Producer a m ()
merge compare producers = do
    xs <- lift $ rights <$> mapM Pipes.next producers
    go xs
  where go :: (Monad m, Functor m, Ord a) => [(a, Producer a m ())] -> Producer a m ()
        go [] = return ()
        go xs = do let (a,producer):xs' = sortBy (compare `on` fst) xs
                   yield a
                   x' <- lift $ next producer
                   go $ either (const xs') (:xs') x'
