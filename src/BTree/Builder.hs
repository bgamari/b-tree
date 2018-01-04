{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BTree.Builder
    ( buildNodes, putBS
    , fromOrderedToFile
    , fromOrderedToByteString
    ) where

import Control.Monad.Trans.State.Strict
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad

import Data.Foldable as F
import qualified Data.Sequence as Seq
import           Data.Sequence (Seq)

import Data.Word
import Data.Ratio
import Control.Lens
import System.IO

import qualified Data.Binary as B
import           Data.Binary (Binary)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Vector as V

import Pipes
import Pipes.Core
import qualified Pipes.Internal as PI

import BTree.Types

-- | A Producer which accepts offsets for the yielded objects in return
type DiskProducer a = Proxy X () (OnDisk a) a

putBS :: (Binary a, Monad m) => a -> Proxy (OnDisk a) a () LBS.ByteString m r
putBS a0 = {-# SCC "putBS" #-} evalStateT (go a0) 0
  where
    go a = do
        s <- get
        let bs = B.encode a
        put $! s + fromIntegral (LBS.length bs)
        lift $ yield bs
        a' <- lift $ request (OnDisk s)
        go a'
{-# INLINE putBS #-}

data DepthState k e = DepthS { -- | nodes to be included in the active node
                               _dNodes       :: !(Seq (k, OnDisk (BTree k OnDisk e)))
                               -- | the length of @dNodes@
                             , _dNodeCount   :: !Int
                               -- | the desired number of elements to fill the active node
                             , _dMinFill     :: [Int]
                             }
makeLenses ''DepthState

next' :: (Monad m) => Proxy X () a' a m r -> m (Either r (a, a' -> Proxy X () a' a m r))
next' = go
  where
    go p = case p of
      PI.Request _ fu -> go (fu ())
      PI.Respond a fu -> return (Right (a, fu))
      PI.M         m  -> m >>= go
      PI.Pure    r    -> return (Left r)
{-# INLINE next' #-}

-- | Compute the optimal node sizes for each stratum of a tree of
-- given size and order
optimalFill :: Order -> Size -> [[Int]]
optimalFill order size = go size
  where
    go :: Word64 -> [[Int]]
    go 0 = error "BTree.Builder.optimalFill: zero size"
    go n =
      let nNodes = ceiling (n % order')
          order' = fromIntegral order :: Word64
          nodes = let (nPerNode, leftover) = n `divMod` nNodes
                  in zipWith (+) (replicate (fromIntegral nNodes) (fromIntegral nPerNode))
                                 (replicate (fromIntegral leftover) 1 ++ repeat 0)
          rest = case nNodes of
                   1  -> []
                   _  -> go nNodes
      in nodes : rest

type BuildM k e m a = StateT [DepthState k e] (DiskProducer (BTree k OnDisk e) m) a

-- | Given a producer of a known number of leaves, produces an optimal B-tree.
-- Technically the size is only an upper bound: the producer may
-- terminate before providing the given number of leaves although the resulting
-- tree will break the minimal fill invariant.
buildNodes :: forall m k e r. Monad m
           => Order -> Size
           -> DiskProducer (BLeaf k e) m r
           -> DiskProducer (BTree k OnDisk e) m (Size, Maybe (OnDisk (BTree k OnDisk e)))
buildNodes order size = {-# SCC "buildNodes" #-}
    flip evalStateT initialState . loop size
  where
    initialState = map (DepthS Seq.empty 0) $ optimalFill order size
    -- depth=0 denotes the bottom (leaves) of the tree.
    loop :: Size -> DiskProducer (BLeaf k e) m r
         -> BuildM k e m (Size, Maybe (OnDisk (BTree k OnDisk e)))
    loop n producer = do
        _next <- lift $ lift $ next' producer
        case _next of
            Left _  -> do
                flushAll (size - n)
            Right _ | n == 0 -> do
                flushAll (size - n)
            Right (leaf@(BLeaf k _), producer') -> do
                -- TODO: Is there a way to check this coercion with the type system?
                OnDisk offset <- processNode k $ Leaf leaf
                loop (n-1) $ producer' (OnDisk offset)

    isFilled :: BuildM k e m Bool
    isFilled = zoom (singular _head) $ do
        nodeCount <- use dNodeCount
        minFill:_ <- use dMinFill
        return $ nodeCount >= minFill

    emitNode :: BuildM k e m (OnDisk (BTree k OnDisk e))
    emitNode = do
        (k0,node0):nodes <- zoom (singular _head) $ do
            nodes <- uses dNodes F.toList
            dNodes .= Seq.empty
            dNodeCount .= 0
            dMinFill %= tail
            return nodes

        -- We used to check the invariants of (not $ null nodes), however this
        -- is wrong. nodes may be empty, for instance, when we are call from
        -- the [_] branch of flushAll.

        let newNode = Node node0 (V.fromList nodes)
        s <- get
        case s of
            [_] -> lift $ respond newNode
            _   -> zoom (singular _tail) $ processNode k0 newNode

    processNode :: k -> BTree k OnDisk e
                -> BuildM k e m (OnDisk (BTree k OnDisk e))
    processNode startKey tree = do
        filled <- isFilled
        when filled $ void $ emitNode
        offset <- lift $ respond tree
        zoom _head $ do
            dNodes %= (Seq.|> (startKey, offset))
            dNodeCount += 1
        return offset

    flushAll :: Size
             -> BuildM k e m (Size, Maybe (OnDisk (BTree k OnDisk e)))
    flushAll 0 = return (0, Nothing)
    flushAll realSize = do
        s <- get
        case s of
            []   -> error "BTree.Builder.flushAll: We should never get here"
            [_]  -> do -- We are at the top node, this shouldn't be flushed yet
                       root <- emitNode
                       return (realSize, Just root)
            d:_  -> do when (not $ Seq.null $ d^.dNodes) $ void $ emitNode
                       zoom (singular _tail) $ flushAll realSize
{-# INLINE buildNodes #-}

-- | Produce a bytestring representing the nodes and leaves of the
-- B-tree and return a suitable header
buildTree :: (Monad m, Binary e, Binary k)
          => Order -> Size
          -> Producer (BLeaf k e) m r
          -> Producer LBS.ByteString m (BTreeHeader k e)
buildTree order size  producer
  | size < 0  = error "BTree.buildTree: Invalid tree size"
  | size == 0 = return zeroSizedHeader
  | otherwise = do
    (realSize, root) <- dropUpstream $ buildNodes order size (dropUpstream producer) >>~ putBS
    if realSize == 0
      then return zeroSizedHeader
      else return $ BTreeHeader magic 1 order realSize root
  where
    zeroSizedHeader = BTreeHeader magic 1 order 0 Nothing
{-# INLINE buildTree #-}

dropUpstream :: Monad m => Proxy X () () b m r -> Proxy X () b' b m r
dropUpstream = {-# SCC "dropUpstream" #-} go
  where
    go producer = do
        n <- lift $ next producer
        case n of
            Left r               -> return r
            Right (a, producer') -> respond a >> go producer'
{-# INLINE dropUpstream #-}

-- | Build a B-tree into the given file.
--
-- As the name suggests, this requires that the @Producer@ emits
-- leaves in ascending key order.
fromOrderedToFile :: (MonadMask m, MonadIO m, Binary e, Binary k)
                  => Order                     -- ^ Order of tree
                  -> Size                      -- ^ Maximum tree size
                  -> FilePath                  -- ^ Output file
                  -> Producer (BLeaf k e) m r  -- ^ 'Producer' of elements
                  -> m ()
fromOrderedToFile order size fname producer =
    bracket (liftIO $ openFile fname WriteMode) (liftIO . hClose) $ \h -> do
        liftIO $ LBS.hPut h $ B.encode invalidHeader
        hdr <- runEffect $ for (buildTree order size producer) $ liftIO . LBS.hPut h
        liftIO $ hSeek h AbsoluteSeek 0
        liftIO $ LBS.hPut h $ B.encode hdr
  where
    invalidHeader = BTreeHeader 0 0 0 0 Nothing
{-# INLINE fromOrderedToFile #-}

-- | Build a B-tree into @ByteString@
--
-- As the name suggests, this requires that the @Producer@ emits
-- leaves in ascending key order.
--
-- This is primarily used for testing. In particular, note that
-- this is a bad idea for large trees as the entire contents of the
-- tree will need to be kept in memory until all leaves have been
-- added so that the header can be prepended.
fromOrderedToByteString :: (Monad m, Binary e, Binary k)
                        => Order                     -- ^ Order of tree
                        -> Size                      -- ^ Maximum tree size
                        -> Producer (BLeaf k e) m r  -- ^ 'Producer' of elements
                        -> m LBS.ByteString
fromOrderedToByteString order size producer = do
    (bs, hdr) <- foldR LBS.append LBS.empty id $ buildTree order size producer
    return $ B.encode hdr `LBS.append` bs

-- | Like @Pipes.Prelude.fold@ but provides returns producer result
-- in addition to accumulator
foldR :: Monad m => (x -> a -> x) -> x -> (x -> b) -> Producer a m r -> m (b, r)
foldR step begin done p0 = loop p0 begin
  where
    loop p x = case p of
        PI.Request _  fu -> loop (fu ()) x
        PI.Respond a  fu -> loop (fu ()) $! step x a
        PI.M          m  -> m >>= \p' -> loop p' x
        PI.Pure    r     -> return (done x, r)
