import Test.QuickCheck
import qualified Data.Map as M
import Data.Foldable (foldl')
import Data.Binary (Binary)
import Pipes hiding (discard)
import qualified BTree as BT

mapToBLeafs :: M.Map k v -> [BT.BLeaf k v]
mapToBLeafs = map (\(k,v)->BT.BLeaf k v) . M.toAscList

test :: (Ord k, Eq v, Binary k, Binary v) => M.Map k v -> Property
test m = ioProperty $ do
    let len = fromIntegral $ M.size m
    bs <- BT.fromOrderedToByteString 10 len (each $ mapToBLeafs m)
    let Right bt = BT.fromByteString bs
    return $ all (\(k,v)->BT.lookup bt k == Just v) (M.assocs m)

main = quickCheck (test :: M.Map Int Int -> Property)
