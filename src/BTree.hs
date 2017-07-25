-- | This package provides immutable B* trees targetting large data
-- sets requiring secondary storage.
module BTree ( -- * Basic types
               BLeaf(..)
             , Size
             , Order
               -- * Building trees
             , fromOrderedToFile
             , fromOrderedToByteString
             , fromUnorderedToFile
               -- * Looking up in trees
             , LookupTree
             , open
             , fromByteString
             , lookup
             , size
               -- * Merging trees
             , mergeTrees
             , mergeLeaves
             , sizedProducerForTree
               -- * Iterating over leaves
             , walkLeaves
             ) where

import Prelude hiding (lookup)
import BTree.Types
import BTree.Merge
import BTree.Builder
import BTree.Lookup
import BTree.Walk
import BTree.BuildUnordered
