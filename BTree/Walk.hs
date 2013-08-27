module BTree.Walk ( walkLeaves
                  , walkNodes
                  , walkNodesWithOffset
                  ) where

import BTree.Types
import qualified Data.ByteString.Lazy as LBS
import Pipes
import qualified Pipes.Prelude as PP
import Data.Binary
import Data.Binary.Get (runGetOrFail)

walkLeaves :: Monad m => Pipe LBS.ByteString (k,v) m ()
walkLeaves = undefined

walkNodes :: (Binary k, Binary v, Monad m)
          => LBS.ByteString -> Producer (BTree k OnDisk v) m (LBS.ByteString, Maybe String)
walkNodes b = walkNodesWithOffset b >-> PP.map snd

walkNodesWithOffset :: (Binary k, Binary v, Monad m)
                    => LBS.ByteString
                    -> Producer (Offset, BTree k OnDisk v) m (LBS.ByteString, Maybe String)
walkNodesWithOffset = go 0
  where go offset bs =
            case runGetOrFail get bs of
              Left (rest,_,err)  -> return (rest, Just err)
              Right (rest,o,a)   -> do
                yield (offset, a)
                if LBS.null rest
                  then return (rest, Nothing)
                  else go (offset+o) rest
