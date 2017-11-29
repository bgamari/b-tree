import Test.QuickCheck
import Test.Tasty
import Test.Tasty.QuickCheck
import qualified Data.Map as M
import Data.Foldable (foldl', foldMap)
import Data.Binary (Binary)
import Pipes hiding (discard)
import qualified Pipes.Prelude as PP
import qualified BTree as BT

mapToBLeafs :: M.Map k v -> [BT.BLeaf k v]
mapToBLeafs = map (\(k,v)->BT.BLeaf k v) . M.toAscList

mapFromBLeafs :: Ord k => [BT.BLeaf k v] -> M.Map k v
mapFromBLeafs = foldMap (\(BT.BLeaf k v) -> M.singleton k v)

-- | Test lookup correctness of exact-sized input
testExact :: (Ord k, Eq v, Binary k, Binary v)
          => M.Map k v -> Property
testExact m = ioProperty $ do
    let len = fromIntegral $ M.size m
    bs <- BT.fromOrderedToByteString 10 len (each $ mapToBLeafs m)
    let Right bt = BT.fromByteString bs
    return $ all (\(k,v)->BT.lookup bt k == Just v) (M.assocs m)

-- | Test lookup correctness of inexact-sized input
test :: (Ord k, Eq v, Binary k, Binary v)
     => BT.Size -> M.Map k v -> Property
test size m = ioProperty $ do
    bs <- BT.fromOrderedToByteString 10 size (each $ mapToBLeafs m)
    let Right bt = BT.fromByteString bs
    return $ all (\(k,v)->BT.lookup bt k == Just v) (take (fromIntegral size) $ M.toAscList m)

testWalk :: (Ord k, Eq v, Binary k, Binary v)
         => Positive Int -> M.Map k v -> Property
testWalk (Positive size) m = ioProperty $ do
    bs <- BT.fromOrderedToByteString 10 (fromIntegral size) (each $ mapToBLeafs m)
    let Right bt = BT.fromByteString bs
        xs = map (\(BT.BLeaf k v) -> (k, v)) $ PP.toList $ void $ BT.walkLeaves bt
    return $ xs == take size (M.toAscList m)

main :: IO ()
main = defaultMain $ testGroup "tests"
    [ testGroup "lookup"
      [ testProperty "inexact size" (test :: BT.Size -> M.Map Int Int -> Property)
      , testProperty "exact size" (testExact :: M.Map Int Int -> Property)
      ]
    , testGroup "walk"
      [ testProperty "walk" (testWalk :: Positive Int -> M.Map Int Int -> Property)
      ]
    ]
