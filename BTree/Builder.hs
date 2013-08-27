{-# LANGUAGE TemplateHaskell #-}              

module BTree.Builder
    ( buildNodes, putBS
    , fromOrdered
    ) where

import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import Control.Monad

import Data.Foldable as F
import qualified Data.Sequence as Seq
import           Data.Sequence (Seq)

import Data.Int
import Data.Ratio
import Control.Lens
import System.IO

import qualified Data.Binary as B
import qualified Data.Binary.Put as Put
import           Data.Binary (Binary)

import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import           Data.ByteString.Lazy (ByteString)

import Pipes
import Pipes.Core
import qualified Pipes.Internal as PI
import qualified Pipes.Prelude as PP

import BTree.Types

-- | A Producer which accepts offsets for the yielded objects in return
type DiskProducer a = Proxy X () (OnDisk a) a

putBS :: (Binary a, Monad m) => Proxy (OnDisk a) a () LBS.ByteString m r
putBS = evalStateT (forever go) 0
  where go = do s <- get
                a <- lift $ request (OnDisk s)
                let bs = B.encode a
                put $! s + fromIntegral (LBS.length bs)
                lift $ yield bs

type Depth = Int

data DepthState k e = DepthS { _dNodes      :: !(Seq (k, OnDisk (BTree k OnDisk e)))
                             , _dNodeCount  :: !Int
                             , _dMinFill    :: [Int]
                             }
makeLenses ''DepthState

data WithDepth a = WithDepth !Depth !a

next' :: (Monad m) => Proxy X () a' a m r -> m (Either r (a, a' -> Proxy X () a' a m r))
next' = go
  where
    go p = case p of
        PI.Request _ fu -> go (fu ())
        PI.Respond a fu -> return (Right (a, fu))
        PI.M         m  -> m >>= go
        PI.Pure    r    -> return (Left r)
     
-- | Compute the optimal node sizes for each stratum of a tree of
-- given size and order
optimalFill :: Order -> Size -> [[Int]]
optimalFill order size = go (fromIntegral size)
  where go n =
          let (nNodes,r) = properFraction $ realToFrac n / realToFrac order
              order' = fromIntegral order
              nodes = replicate nNodes order'
                      ++ case numerator r of
                           0   -> []
                           n   -> [n * order' `div` denominator r]
              nNodes' = nNodes
                        + case numerator r of 
                            0   -> 0
                            _   -> 1
              rest = case nNodes' of
                       1  -> []
                       _  -> go nNodes'
          in nodes : rest

-- | Given a producer of a known number of leafs, produces an optimal B-tree.
-- Technically the size is only an upper bound: the producer may
-- terminate before providing the given number of leafs although the resulting
-- tree will break the minimal fill invariant.
--
-- depth=0 denotes the bottom (leafs) of the tree.
buildNodes :: Monad m
           => Order -> Size
           -> DiskProducer (BLeaf k e) m r
           -> DiskProducer (BTree k OnDisk e) m (BTreeHeader k e)
buildNodes order size =
    flip evalStateT initialState . loop size
  where initialState = map (DepthS Seq.empty 0) $ optimalFill order size
        loop :: Monad m
             => Size -> DiskProducer (BLeaf k e) m r
             -> StateT [DepthState k e] (DiskProducer (BTree k OnDisk e) m)
                       (BTreeHeader k e)
        loop n producer = do
            _next <- lift $ lift $ next' producer
            case _next of
              Left r  -> do
                flushAll n
              Right (leaf, producer') | n == 0 -> do
                flushAll n
              Right (leaf, producer')  -> do
                -- TODO: Is there a way to check this coercion with the type system?
                OnDisk offset <- processNode $ Leaf leaf
                loop (n-1) $ producer' (OnDisk offset)

        isFilled :: Monad m
                 => StateT [DepthState k e] m Bool
        isFilled = zoom (singular _head) $ do
            nodeCount <- use dNodeCount
            minFill:_ <- use dMinFill
            return $ nodeCount >= minFill

        emitNode :: Monad m
                 => StateT [DepthState k e] (DiskProducer (BTree k OnDisk e) m) (OnDisk (BTree k OnDisk e))
        emitNode = do
            (_,node0):nodes <- zoom (singular _head) $ do
                nodes <- uses dNodes F.toList
                dNodes .= Seq.empty
                dNodeCount .= 0
                dMinFill %= tail
                return nodes
            s <- get
            let newNode = Node node0 nodes
            case s of
              [x] -> lift $ respond newNode
              _   -> zoom (singular _tail) $
                         processNode newNode

        processNode :: Monad m
                    => BTree k OnDisk e
                    -> StateT [DepthState k e]
                              (DiskProducer (BTree k OnDisk e) m)
                              (OnDisk (BTree k OnDisk e))
        processNode tree = do
            filled <- isFilled
            when filled $ void $ emitNode
            offset <- lift $ respond tree
            zoom _head $ do
                dNodes %= (Seq.|> (treeStartKey tree, offset))
                dNodeCount += 1
            return offset

        flushAll :: Monad m
                 => Size
                 -> StateT [DepthState k e]
                           (DiskProducer (BTree k OnDisk e) m)
                           (BTreeHeader k e)
        flushAll realSize = do
            s <- get
            case s of
              [_]  -> do -- We are at the top node, this shouldn't be flushed yet
                         root <- emitNode
                         return $ BTreeHeader magic 1 order realSize root
              d:_  -> do when (not $ Seq.null $ d^.dNodes) $ void $ emitNode
                         zoom (singular _tail) $ flushAll realSize

-- | Produce a bytestring representing the nodes and leafs of the 
-- B-tree and return a suitable header
buildTree :: (Monad m, Binary e, Binary k)
          => Order -> Size
          -> Producer (BLeaf k e) m r
          -> Producer LBS.ByteString m (BTreeHeader k e)
buildTree order size producer =
    dropUpstream $ buildNodes order size (dropUpstream producer) >>~ const putBS

dropUpstream :: Monad m => Proxy X () () b m r -> Proxy X () b' b m r
dropUpstream = go
  where go producer = do
          n <- lift $ next producer
          case n of
            Left r               -> return r
            Right (a, producer') -> respond a >> go producer'
          
-- | Build a B-tree into the given file
fromOrdered :: (Binary e, Binary k)
            => Order -> Size
            -> FilePath
            -> Producer (BLeaf k e) IO r
            -> IO ()
fromOrdered order size fname producer =
    withFile fname WriteMode $ \h->do
    LBS.hPut h $ B.encode invalidHeader
    hdr <- run $ for (buildTree order size producer) $ lift . LBS.hPut h
    hSeek h AbsoluteSeek 0
    LBS.hPut h $ B.encode hdr
    return ()
  where invalidHeader = BTreeHeader 0 0 0 0 (OnDisk 0)
