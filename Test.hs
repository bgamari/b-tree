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
things = [BLeaf (i) (1000*i) | i <- [0..n-1]]

open' = L.open :: FilePath -> IO (Either String (LookupTree Int64 Int64))

main = do
    fromOrderedToFile 4 (fromIntegral n) "hello1.btree" (each things)
    fromOrderedToFile 4 (fromIntegral n) "hello2.btree" (each things)
    
    Right a <- open' "hello1.btree"
    Right b <- open' "hello2.btree"
    mergeTrees compare (+) 4 "hello-merged.btree" [a,b]
    
    Right lt <- open' "hello-merged.btree"
    mapM_ (print . L.lookup lt) [0..n]
