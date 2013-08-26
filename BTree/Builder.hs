{-# LANGUAGE TemplateHaskell #-}              

module BTree.Builder
    ( 
    ) where

import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import Control.Monad

import Data.Foldable as F
import qualified Data.Sequence as Seq
import           Data.Sequence (Seq)
import Data.Int
import Control.Lens

import qualified Data.Binary as B
import           Data.Binary (Binary)

import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import           Data.ByteString.Lazy (ByteString)

import Pipes
import Pipes.Core (respond)
import qualified Pipes.Internal as PI

import BTree.Types

{-       
fromOrdered :: (Monad m, Ord k, Binary k, Binary v)
                => Int -> Pipe (k,v) ByteString m r
fromOrdered order = flip evalStateT 0 $ do
    putStream $ B.encode header
    forever leafs
  where header = BTreeHeader magic (fromIntegral order) 0

        leafs :: (Monad m, Binary k, Binary v)
              => StateT Offset (Pipe (k,v) ByteString m) [Offset]
        leafs = replicateM order $ do
                   (k,v) <- lift await
                   putStream $ B.encode (k,v)
        
        putStream :: (Monad m)
                  => ByteString
                  -> StateT Offset (Pipe (k,v) ByteString m) Offset
        putStream bs = do
            offset <- get
            lift $ yield bs
            modify (+ LBS.length bs)
            return offset
-}

type Depth = Int

data DepthState k e = DepthS { _dNodes      :: !(Seq (k, OnDisk (BTree k OnDisk e)))
                             , _dNodeCount  :: !Int
                             , _dMinFill    :: [Int]
                             }
makeLenses ''DepthState

-- | A Producer which accepts offsets for the yielded objects in return
type DiskProducer a = Proxy X () (OnDisk a) a

data WithDepth a = WithDepth !Depth !a

next' :: (Monad m) => Proxy X () a' a m r -> m (Either r (a, a' -> Proxy X () a' a m r))
next' = go
  where
    go p = case p of
        PI.Request _ fu -> go (fu ())
        PI.Respond a fu -> return (Right (a, fu))
        PI.M         m  -> m >>= go
        PI.Pure    r    -> return (Left r)
     
buildNodes :: Monad m
           => Order -> Size
           -> DiskProducer (BTree k OnDisk e) m r
           -> DiskProducer (BTree k OnDisk e) m r
buildNodes order size =
    flip evalStateT (map initialState [0..maxDepth]) . loop
  where loop :: Monad m
             => DiskProducer (BTree k OnDisk e) m r
             -> StateT [DepthState k e] (DiskProducer (BTree k OnDisk e) m) r
        loop producer = do
            n <- lift $ lift $ next' producer
            case n of
              Left r  -> do
                flushAll
                return r
              Right (tree, producer')  -> do
                offset <- processNode tree
                loop $ producer' offset

        initialState depth = DepthS Seq.empty 0 $ cycle [2] -- FIXME
        minFill = (order + 1) `div` 2
        maxDepth = ceiling $ log (realToFrac size) / log (realToFrac order)

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
            zoom (singular _tail) $
                processNode $ Node node0 nodes

        processNode :: Monad m
                    => BTree k OnDisk e
                    -> StateT [DepthState k e]
                              (DiskProducer (BTree k OnDisk e) m)
                              (OnDisk (BTree k OnDisk e))
        processNode tree = do
            offset <- lift $ respond tree
            zoom _head $ do
                dNodes %= (Seq.|> (treeStartKey tree, offset))
                dNodeCount += 1
            filled <- isFilled
            when filled $ void $ emitNode
            return offset

        flushAll :: Monad m
                 => StateT [DepthState k e]
                           (DiskProducer (BTree k OnDisk e) m)
                           (OnDisk (BTree k OnDisk e))
        flushAll = do
            s <- get
            let depthDone :: DepthState k e -> Bool
                depthDone (DepthS _ 0 _) = True
                depthDone _              = False
            n <- emitNode
            if depthDone (head $ tail s)
              then return n
              else do emitNode
                      zoom (singular _tail) flushAll
