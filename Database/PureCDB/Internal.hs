module Database.PureCDB.Internal where

import System.IO
import qualified Data.Vector as V
import Data.Word
import qualified Data.ByteString as B
import Data.Bits
import qualified Data.IntMap as I
import Data.List (foldl')

data TOCHash = TOCHash { hPosition :: Word32, hLength :: Word32 } deriving (Show)

-- | Read handle for the database.
data ReadCDB = ReadCDB { rHandle :: Handle, rTOC :: V.Vector TOCHash }

cdbHash :: B.ByteString -> Word32
cdbHash = B.foldl' go 5381 where go h c = ((h `shiftL` 5) + h) `xor` (fromIntegral c)

tocIndex :: Integral a => a -> Int
tocIndex h = fromIntegral $ h `mod` 256

tocFind :: ReadCDB -> B.ByteString -> (TOCHash, Word32)
tocFind (ReadCDB _ vec) bs = (vec V.! tocIndex h, h) where h = cdbHash bs 

layoutHash :: Int -> I.IntMap [a] -> Maybe [(Int, a)]
layoutHash sz = test . concat . foldl' go [] . I.toList where
    go l@(((li, _):_):_) p = (one li p):l
    go [] p = [one (-1) p]
    go ([]:_) _ = error "layoutHash: found empty list"
    one lastIdx (idx, as) = reverse $ zip [ (max idx $ lastIdx + 1) .. ] as
    test [] = Just []
    test l@(x:_) = if fst x >= sz then Nothing else Just l

coalesceHash :: Integral a => a -> [(a, b)] -> I.IntMap [(a, b)]
coalesceHash hlen = foldl' go I.empty where
    go im (a, b) = I.insertWith (++) (fromIntegral $ hashSlot a hlen) [(a, b)] im

createHashVector :: Integral a => a -> (a, b) -> [(a, b)] -> V.Vector (a, b)
createHashVector sz start l = maybe (createHashVector next start l) vec hsh
    where vec ups = V.replicate (fromIntegral sz) start V.// ups
          hsh = layoutHash (fromIntegral sz) $ coalesceHash sz l
          next = 1 + sz + (sz `div` 3)

hashSlot :: Integral a => a -> a -> a
hashSlot h hlen = (h `div` 256) `mod` hlen

