import qualified Data.ByteString.Lazy as LBS
import Pipes       
import BTree.Walk
import BTree.Types
import Data.Binary
import Data.Binary.Get
import System.Environment
       
main = do
    fname:_ <- getArgs
    r <- LBS.readFile fname
    let Right (rest,_,hdr) = runGetOrFail get r
    print (hdr :: BTreeHeader Int Int)
    result <- run $ for (walkNodesWithOffset rest)
      $ lift . (print :: (Offset, BTree Int OnDisk Int) -> IO ())
    print result

