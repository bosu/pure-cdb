-- |
-- Module      : Database.CDB
-- Copyright   : (c) Boris Sukholitko 2013
--
-- License     : BSD3
--
-- A library for reading and writing CDB (Constant Database) files.
--
-- CDB files are immutable key-value stores, designed for extremely fast and
-- memory-efficient construction and lookup. They can be as large as 4GB, and
-- at no point in their construction or use must all data be loaded into
-- memory. CDB files can contain multiple values for a given key.
--
-- For more information on the CDB file format, please see:
--     <http://cr.yp.to/cdb.html>
--
-- Here's how you make new CDB file:
--  
-- > import qualified Data.ByteString.Char8 as B
-- > import Database.PureCDB
-- >
-- > makeIt :: IO ()
-- > makeIt = makeCDB (do
-- >       addBS (B.pack "foo") (B.pack "bar")
-- >       addBS (B.pack "foo") (B.pack "baz")) "foo.cdb"
--
-- You can later use it as in:
--
-- > getIt :: IO [ByteString]
-- > getIt = do
-- >       f <- openCDB "foo.cdb"
-- >       bs <- getBS f (B.pack "foo")
-- >       closeCDB "foo.cdb"
-- >       return bs
-- >
--
-- @getIt@ returns [ \"bar\", \"baz\" ] in unspecified order.
--
-- Note that @pure-cdb@ works on strict 'ByteString''s only for now.

{-# LANGUAGE DeriveFunctor, GeneralizedNewtypeDeriving #-}
module Database.PureCDB (
    -- * Writing interface
    WriteCDB, makeCDB, addBS
    
    -- * Reading interface
    , ReadCDB, openCDB, closeCDB, getBS) where

import Data.Word
import System.IO
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.Vector as V
import qualified Data.Vector.Generic.Mutable as MV
import Control.Applicative
import Control.Monad.State
import System.Directory
import Control.Monad.ST
import Database.PureCDB.Internal

data HashState = HashState { hsCount :: !Word32, hsPairs :: ![(Word32, Word32)] }
data WriteState = WriteState { wsHandle :: Handle, wsTOC :: !(V.Vector HashState) }

-- | Write context monad transformer.
newtype WriteCDB m a = WriteCDB (StateT WriteState m a)
                            deriving (Functor, Monad, Applicative, MonadTrans, MonadIO)

readWordPairs :: Handle -> Int -> IO [(Word32, Word32)]
readWordPairs ioh sz = do
    bs <- BL.hGet ioh sz
    return $ runGet (mapM (const go) [ 1 .. sz `div` 8 ]) bs
    where go = (,) <$> getWord32le <*> getWord32le

-- | Opens CDB database.
openCDB :: FilePath -> IO ReadCDB
openCDB fp = do
    ioh <- openBinaryFile fp ReadMode
    hSetBuffering ioh NoBuffering
    wps <- readWordPairs ioh 2048
    let v = V.fromList $ map (uncurry TOCHash) wps
    return $ ReadCDB ioh v

-- | Closes the database.
closeCDB :: ReadCDB -> IO ()
closeCDB (ReadCDB ioh _) = hClose ioh

getRecord :: Handle -> Word32 -> IO (B.ByteString, B.ByteString)
getRecord ioh sk = do
    hSeek ioh AbsoluteSeek (fromIntegral sk)
    [(ksz, vsz)] <- readWordPairs ioh 8
    k <- B.hGet ioh $ fromIntegral ksz
    v <- B.hGet ioh $ fromIntegral vsz
    return (k, v)

-- | Fetches key from the database.
getBS :: ReadCDB -> B.ByteString -> IO [B.ByteString]
getBS r@(ReadCDB ioh _) bs = do
    hSeek ioh AbsoluteSeek (fromIntegral $ hpos + slot * 8)
    wps <- readWordPairs ioh (fromIntegral $ (hlen - slot) * 8)
    let pairs = filter ((== h) . fst) $ takeWhile ((/= 0) . snd) wps
    kvs <- mapM (getRecord ioh . snd) pairs
    return $ map snd $ filter ((bs ==) . fst) kvs
    where (TOCHash hpos hlen, h) = tocFind r bs
          slot = hashSlot h hlen

updateTOC :: V.Vector HashState -> B.ByteString -> Word32 -> V.Vector HashState
updateTOC vec key cur = runST $ do
    v <- V.unsafeThaw vec
    hs <- MV.read v i
    MV.write v i (HashState (cnt hs) (pairs hs))
    V.unsafeFreeze v
    where hsh = cdbHash key
          cnt hs = hsCount hs + 1
          i = tocIndex hsh
          pairs hs = (hsh, cur):hsPairs hs

-- | Adds key and value to the CDB database.
addBS :: MonadIO m => B.ByteString -> B.ByteString -> WriteCDB m ()
addBS key val = WriteCDB $ do
    st <- get
    cur <- liftIO $ hTell (wsHandle st)
    liftIO $ BL.hPut (wsHandle st) buf
    put $ st { wsTOC = updateTOC (wsTOC st) key (fromIntegral cur) }
    where buf = runPut $ do
                putWord32le $ fromIntegral $ B.length key
                putWord32le $ fromIntegral $ B.length val
                putByteString key
                putByteString val

writePairs :: Handle -> [(Word32, Word32)] -> IO ()
writePairs ioh pairs = BL.hPut ioh buf where
    buf = runPut $ mapM_ one pairs
    one (a, b) = putWord32le a >> putWord32le b

writeOneHash :: Handle -> HashState -> IO (Word32, Word32)
writeOneHash ioh (HashState cnt pairs) = do
    cur <- hTell ioh
    writePairs ioh $ V.toList vec
    return (fromIntegral cur, fromIntegral $ V.length vec)
    where vec = createHashVector (1 + cnt * 2) (0, 0) pairs

-- | Runs WriteCDB monad transformer to make the database.
makeCDB :: MonadIO m => WriteCDB m a -> FilePath -> m a
makeCDB (WriteCDB m) fp = do
    ioh <- liftIO $ openBinaryFile (fp ++ ".tmp") WriteMode
    liftIO $ hSeek ioh AbsoluteSeek 2048
    (res, st) <- runStateT m $ WriteState ioh $ V.replicate 256 (HashState 0 [])
    tocs <- liftIO $ mapM (writeOneHash ioh) $ V.toList $ wsTOC st
    liftIO $ hSeek ioh AbsoluteSeek 0
    liftIO $ writePairs ioh tocs
    liftIO $ hClose ioh
    liftIO $ renameFile (fp ++ ".tmp") fp
    return res
