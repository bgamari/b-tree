import Data.Binary (decode)                
import qualified Data.ByteString.Lazy as LBS
import Control.Monad.IO.Class
import Control.Monad (void)
import Data.Int

import Pipes
import Pipes.Prelude as PP
import Pipes.Core

import BTree.Walk
import BTree.Types
import BTree.Builder

printUpstream :: (MonadIO m, Show a') => Producer a m r -> Proxy X () a' a m r
printUpstream = go
  where go producer = do              
          n <- lift $ next producer
          case n of 
            Left r                -> return r
            Right (a, producer')  -> do a' <- respond a
                                        liftIO $ print a'
                                        go producer'

main = do
    let n = 95
        leaves = [ BLeaf i (1000*i) | i <- [0..n-1] ] :: [BLeaf Int64 Int64]
        src :: Proxy X () (OnDisk (BLeaf Int64 Int64)) (BLeaf Int64 Int64) IO ()
        src = printUpstream $ each leaves
    r <- PP.fold LBS.append LBS.empty id
             $ void (buildNodes 10 (fromIntegral n) src) >>~ const putBS
    print r
    run $ for (walkNodes r) $ lift . (print :: BTree Int64 OnDisk Int64 -> IO ())
    return ()

decodeNode :: LBS.ByteString -> BTree Int64 OnDisk Int64
decodeNode = decode           

