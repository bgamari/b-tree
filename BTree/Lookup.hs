module BTree.Lookup ( LookupTree
                    , open
                    , lookup
                    ) where

import Prelude hiding (lookup)
import Control.Error
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Binary
import System.IO.MMap
import BTree.Types

data LookupTree k e = LookupTree { ltData    :: !BS.ByteString
                                 , ltHeader  :: !(BTreeHeader k e)
                                 }
     
fetch :: Binary a => LookupTree k e -> OnDisk a -> a
fetch lt (OnDisk offset) =
    decode $ LBS.fromStrict $ BS.drop (fromIntegral offset) (ltData lt)

open :: FilePath -> IO (Either String (LookupTree k e))
open fname = runEitherT $ do
    d <- fmapLT show $ tryIO $ mmapFileByteString fname Nothing
    (rest, _, hdr) <- fmapLT (\(_,_,err)->err) $ EitherT $ return
                      $ decodeOrFail $ LBS.fromStrict d
    return $ LookupTree (LBS.toStrict rest) hdr
    
lookup :: (Binary k, Binary e, Ord k)
       => LookupTree k e -> k -> Maybe e
lookup lt k = go $ fetch lt (btRoot $ ltHeader lt)
  where go (Leaf (BLeaf k' e))
          | k' == k   = Just e
          | otherwise = Nothing
        go (Node c0 children@((k0,_):_))
          | k < k0    = go $ fetch lt c0
          | otherwise = case dropWhile (\(k',_)->k' < k) children of
                          []         -> Nothing
                          (_,tree):_ -> go $ fetch lt tree
