import System.IO                
import qualified Data.ByteString.Lazy as LBS
import BTree.Builder
import BTree.Walk
import BTree.Merge

import Pipes

things :: [(Int,Int)]
things = [(1,4), (2,7), (3,4), (10,3)]

main = do
    h <- openFile "hello.btree" WriteMode
    run $ for (each things >-> fromOrderedList 16)
        $ lift . LBS.hPut h
    hClose h
