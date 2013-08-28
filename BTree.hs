module BTree ( -- * Basic types
               BLeaf(..)
             , Size
             , Order
               -- * Building trees
             , fromOrderedToFile
             , fromOrderedToByteString
               -- * Looking up in trees
             , LookupTree
             , open
             , lookup
               -- * Merging trees
             , mergeTrees
             ) where

import Prelude hiding (lookup)
import BTree.Types
import BTree.Merge
import BTree.Builder
import BTree.Lookup
