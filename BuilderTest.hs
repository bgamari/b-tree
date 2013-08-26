import Data.Binary (decode)                
import qualified Data.ByteString.Lazy as LBS
import BTree.Types
import BTree.Builder
import Pipes
import Pipes.Prelude
import Pipes.Core
import Control.Monad.IO.Class
import Data.Int

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
        leaves = [ Leaf i (OnDisk $ 1000*i) | i <- [0..n-1] ] :: [BTree Int64 OnDisk Int64]
        src :: Proxy X () (OnDisk (BTree Int64 OnDisk Int64)) (BTree Int64 OnDisk Int64) IO ()
        src = printUpstream $ each leaves
    r <- run $ for (buildNodes 10 (fromIntegral n) src >>~ const putBS)
             $ lift . print . decodeNode
    print r
    return ()

decodeNode :: LBS.ByteString -> BTree Int64 OnDisk Int64
decodeNode = decode           
    
-- buildNodes 10 100 src :: Proxy X () (OnDisk a) a
-- putBS                 :: Proxy (OnDisk a) a () ByteString
