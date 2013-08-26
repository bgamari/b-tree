module BTree.Walk (walk) where

import BTree.Types
import qualified Data.ByteString.Lazy as LBS
import Pipes

walk :: Pipe LBS.ByteString (k,v) m ()
walk = undefined
