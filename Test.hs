import Control.Monad
import Control.Monad.Trans.Writer
import BTree.Builder
import BTree.Walk
import BTree.Merge
import BTree.Types
import BTree.Lookup as L
import Pipes
import qualified Data.ByteString.Lazy as BS
import Data.Binary
import Data.Int
import Data.List (sortBy)
import Data.Ord (comparing)

n = 16 :: Size

assocsToLeaves :: Ord k => [(k,v)] -> [BLeaf k v]
assocsToLeaves = map (\(k,v)->BLeaf k v) . sortBy (comparing fst)

assocs = [(i, 1000*i) | i <- [0..n-1]] :: [(Word64, Word64)]
things = assocsToLeaves assocs

open' :: [(k,v)] -> FilePath -> IO (Either String (LookupTree k v))
open' _ = L.open

main = do
    putStrLn "1:" >> buildCheck 4 n assocs
    putStrLn "2:" >> buildCheck 4 (n-5) assocs
    putStrLn "3:" >> buildCheck 4 (2*n) assocs
    putStrLn "4:" >> buildCheck 4 (n `div` 2) assocs
    putStrLn "5:" >> buildCheck 4 (n `div` 3) assocs

    fromOrderedToFile 4 (fromIntegral n) "hello1.btree" (each things)
    fromOrderedToFile 4 (fromIntegral n) "hello2.btree" (each things)
    Right a <- open' assocs "hello1.btree"
    Right b <- open' assocs "hello2.btree"
    mergeTrees compare (\a b->return $ a+b) 4 "hello-merged.btree" [a,b]
    Right lt <- open' assocs "hello-merged.btree"
    mapM_ (print . L.lookup lt) [0..n]


buildCheck :: (Ord k, Eq k, Eq v, Show k, Show v, Binary k, Binary v)
           => Order -> Size -> [(k,v)] -> IO ()
buildCheck order size assocs = do
    fromOrderedToFile order size "test.btree" (each things)
    Right a <- open' assocs "test.btree"
    mapM_ putStrLn $ checkLookupTree (take (fromIntegral size) $ assocs) a

checkLookupTree :: (Ord k, Eq k, Eq v, Show k, Show v, Binary k, Binary v)
                => [(k,v)] -> LookupTree k v -> [String]
checkLookupTree assocs lt = execWriter $
    forM_ assocs $ \(a,b)->do
        case L.lookup lt a of
          Nothing        ->
            tell ["Key "++show a++" was not in map, should have been "++show b]
          Just b' | b /= b' ->
            tell ["Key "++show a++" was "++show b'++", should have been "++show b]
          otherwise      ->
            return ()
