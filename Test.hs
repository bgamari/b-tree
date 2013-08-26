import BTree.Builder
import BTree.Walk
import BTree.Merge
import BTree.Types
import Pipes

things :: [BLeaf Int Int]
things = [BLeaf i 5 | i <- [1..100]]

main = fromOrdered 10 100 "hello.btree" (each things)
