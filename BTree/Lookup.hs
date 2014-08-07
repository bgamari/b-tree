module BTree.Lookup ( LookupTree
                    , open
                    , fromByteString
                    , lookup
                    ) where

import Prelude hiding (lookup)
import Control.Error
import Control.Lens hiding (children)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Binary
import System.IO.MMap
import BTree.Types

fetch :: (Binary a) => LookupTree k e -> OnDisk a -> a
fetch lt (OnDisk offset) =
    decode $ LBS.fromStrict $ BS.drop (fromIntegral offset) (lt^.ltData)

-- | Read a B-tree from a ByteString produced by 'BTree.Builder'
fromByteString :: LBS.ByteString -> Either String (LookupTree k e)
fromByteString bs = do
    (rest, _, hdr) <- fmapL (\(_,_,e)->e) $ decodeOrFail bs
    validateHeader hdr
    return $ LookupTree (LBS.toStrict rest) hdr

-- | Read a B-tree from a file produced by 'BTree.Builder'
open :: FilePath -> IO (Either String (LookupTree k e))
open fname = runEitherT $ do
    d <- fmapLT show $ tryIO $ mmapFileByteString fname Nothing
    EitherT $ return $ fromByteString (LBS.fromStrict d)
   
-- | Lookup a key in a tree
lookup :: (Binary k, Binary e, Ord k)
       => LookupTree k e -> k -> Maybe e
lookup lt k = go $ fetch lt (lt ^. ltHeader . btRoot)
  where go (Leaf (BLeaf k' e))
          | k' == k     = Just e
          | otherwise   = Nothing
        go (Node c0 []) = go $ fetch lt c0 -- is this case necessary?
        go (Node c0 children@((k0,_):_))
          | k < k0      = go $ fetch lt c0
          | otherwise   = case takeWhile (\(k',_)->k' <= k) children of
                            []  -> Nothing
                            xs  -> go $ fetch lt $ snd $ last xs
