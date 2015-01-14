import BTree as BT
import Pipes
import Control.Error
import Control.Monad.Trans.Class
import System.Random
 
main = runEitherT $ do
    let keys :: [Int]
        keys = take (10*1000*1000) $ randoms $ mkStdGen 123
    let prod = each $ map (\k->BLeaf k k) keys
    --let prod = each [BLeaf (i*137 `mod` 653) i | i <- [0..10000000::Int]]
    BT.fromUnorderedToFile "/tmp" 1000000 10 "out.btree" prod

    Right bt <- lift $ BT.open "out.btree"
    lift $ print $ BT.lookup (bt :: LookupTree Int Int) 4
