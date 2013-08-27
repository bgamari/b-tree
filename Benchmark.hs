import BTree as BT
import Criterion.Main
import Pipes

main = do
    buildTree "hello.btree" 100 100000
    Right lt <- BT.open "hello.btree"
             :: IO (Either String (LookupTree Int Int))

    defaultMain $ benchmarks lt

benchmarks lt =
    [ bench "build tree (order 100, 100000 elements)"
      $ nfIO $ buildTree "test.btree" 100 100000
    , bench "build tree (order 100, 1000000 elements)"
      $ nfIO $ buildTree "test.btree" 100 1000000
    , bench "lookup (5000 lookups)"
      $ nf (\lt->map (lookupBench lt) [0..5000]) lt
    ]

buildTree :: FilePath -> Order -> Int -> IO ()
buildTree fname order n = 
    BT.fromOrdered 4 (fromIntegral n) fname (each things)
  where things :: [BLeaf Int Int]
        things = [BLeaf (2*i) i | i <- [0..n-1]]
        
lookupBench :: LookupTree Int Int -> Int -> Maybe Int
lookupBench lt = BT.lookup lt
