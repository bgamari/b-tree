import BTree.Builder
import BTree.Walk
import BTree.Merge
import BTree.Types
import BTree.Lookup as L
import Criterion.Main
import Pipes

main = do
    buildTree "hello.btree" 100000
    Right lt <- L.open "hello.btree"
             :: IO (Either String (LookupTree Int Int))

    defaultMain $ benchmarks lt

benchmarks lt =
    [ bench "build tree" $ nfIO $ buildTree "test.btree" 100000
    , bench "lookup" $ nf (\lt->map (lookupBench lt) [0..5000]) lt
    ]

buildTree :: FilePath -> Int -> IO ()
buildTree fname n = 
    fromOrdered 4 (fromIntegral n) fname (each things)
  where things :: [BLeaf Int Int]
        things = [BLeaf (2*i) i | i <- [0..n-1]]
        
lookupBench :: LookupTree Int Int -> Int -> Maybe Int
lookupBench lt = L.lookup lt
