import qualified Data.ByteString.Lazy as LBS
import Pipes       
import BTree.Walk
import BTree.Types
       
main = do
    r <- LBS.readFile "hello.btree"
    run $ for (walkNodes r) $ lift . (print :: BTree Int OnDisk Int -> IO ())
