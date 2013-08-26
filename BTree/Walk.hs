module BTree.Walk ( walk
                  , walkNodes
                  ) where

import BTree.Types
import qualified Data.ByteString.Lazy as LBS
import Pipes
import Data.Binary
import Data.Binary.Get (runGetOrFail)

walk :: Monad m => Pipe LBS.ByteString (k,v) m ()
walk = undefined

walkNodes :: (Binary k, Binary v, Monad m)
          => LBS.ByteString -> Producer (BTree k OnDisk v) m (LBS.ByteString, Maybe String)
walkNodes = go
  where go bs = case runGetOrFail get bs of
                  Left (rest,_,err)  -> return (rest, Just err)
                  Right (rest,_,a)   -> do
                    yield a
                    if LBS.null rest
                      then return (rest, Nothing)
                      else go rest
