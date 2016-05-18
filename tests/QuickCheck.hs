import Test.QuickCheck
import qualified Data.Map as M
import Data.Foldable (foldl')
import Data.Binary (Binary)
import Pipes hiding (discard)
import qualified BTree as BT

mapToBLeafs :: M.Map k v -> [BT.BLeaf k v]
mapToBLeafs = map (\(k,v)->BT.BLeaf k v) . M.toAscList

-- | Test correctness of exact-sized input
testExact :: (Ord k, Eq v, Binary k, Binary v)
          => M.Map k v -> Property
testExact m = ioProperty $ do
    let len = fromIntegral $ M.size m
    bs <- BT.fromOrderedToByteString 10 len (each $ mapToBLeafs m)
    let Right bt = BT.fromByteString bs
    return $ all (\(k,v)->BT.lookup bt k == Just v) (M.assocs m)

-- | Test correctness of inexact-sized input
test :: (Ord k, Eq v, Binary k, Binary v)
     => BT.Size -> M.Map k v -> Property
test size m = ioProperty $ do
    bs <- BT.fromOrderedToByteString 10 size (each $ mapToBLeafs m)
    let Right bt = BT.fromByteString bs
    return $ all (\(k,v)->BT.lookup bt k == Just v) (take (fromIntegral size) $ M.toAscList m)

main = do
    quickCheck (test :: BT.Size -> M.Map Int Int -> Property)
    quickCheck (testExact :: M.Map Int Int -> Property)
