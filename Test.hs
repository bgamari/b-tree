import BTree.Builder
import BTree.Walk
import BTree.Merge
import BTree.Types
import BTree.Lookup as L
import Pipes
import Data.ByteString.Lazy as BS
import Data.Int

n = 16
things :: [BLeaf Int64 Int64]
things = [BLeaf (i) (0x1000*i) | i <- [0..n-1]]

main = do
    fromOrdered 4 (fromIntegral n) "hello.btree" (each things)

    Right lt <- L.open "hello.btree"
             :: IO (Either String (LookupTree Int64 Int64))
    mapM_ (print . L.lookup lt) [0..n]
