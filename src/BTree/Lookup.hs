module BTree.Lookup ( LookupTree
                    , open
                    , fromByteString
                    , lookup
                    , size
                    ) where

import Prelude hiding (lookup)
import Control.Error
import Control.Lens hiding (children)
import qualified Data.ByteString as BS
import qualified Data.Vector as V
import qualified Data.ByteString.Lazy as LBS
import Data.Binary
import System.IO.MMap
import BTree.Types

fetch :: (Binary a) => LookupTree k e -> OnDisk a -> a
fetch lt (OnDisk offset) =
    decode $ LBS.fromStrict $ BS.drop (fromIntegral offset) (lt^.ltData)

-- | Read a B-tree from a 'ByteString' produced by 'BTree.Builder'
fromByteString :: LBS.ByteString -> Either String (LookupTree k e)
fromByteString bs = do
    (rest, _, hdr) <- fmapL (\(_,_,e)->e) $ decodeOrFail bs
    validateHeader hdr
    return $ LookupTree (LBS.toStrict rest) hdr

-- | Open a B-tree file.
open :: FilePath -> IO (Either String (LookupTree k e))
open fname = runExceptT $ do
    d <- fmapLT show $ tryIO $ mmapFileByteString fname Nothing
    ExceptT $ return $ fromByteString (LBS.fromStrict d)

-- | Lookup a key in a B-tree.
lookup :: (Binary k, Binary e, Ord k)
       => LookupTree k e -> k -> Maybe e
lookup lt k =
    case lt ^. ltHeader . btRoot of
      Just root -> go $ fetch lt root
      Nothing   -> Nothing
  where
    go (Leaf (BLeaf k' e))
      | k' == k     = Just e
      | otherwise   = Nothing
    go (Node c0 children)
      | V.null children = go $ fetch lt c0 -- is this case necessary?
      | let (k0,_) = V.head children
      , k < k0      = go $ fetch lt c0
      | otherwise   =
          case V.takeWhile (\(k',_)->k' <= k) children of
            rest
              | V.null rest -> Nothing
              | otherwise   -> go $ fetch lt $ snd $ V.last rest

-- | How many keys are in a 'LookupTree'.
size :: LookupTree k e -> Size
size = _btSize . _ltHeader
