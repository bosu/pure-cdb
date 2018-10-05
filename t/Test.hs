{-# OPTIONS -fno-warn-unused-do-bind #-}
module Main(main) where

import Test.Simple
import Database.PureCDB
import qualified Data.Vector as V
import Control.Monad.Trans (liftIO)
import qualified Data.ByteString.Char8 as B
import qualified Data.IntMap as I
import System.Unix.Directory (withTemporaryDirectory)
import Database.PureCDB.Internal

main :: IO ()
main = withTemporaryDirectory "/tmp/pure_cdb_XXXXXXX" $ \td -> testSimpleMain $ do
    plan 13
    r <- liftIO $ openCDB "t/foo.cdb"
    is (V.findIndex ((> 0) . hLength) $ rTOC r) (Just 163)
    is ((cdbHash foo) `mod` tOC_ENTRY_NUMBER) 163
    is (hPosition $ fst $ tocFind r foo) (hPosition $ rTOC r V.! 163)

    v <- liftIO $ getBS r foo
    is (map B.unpack v) [ "bar" ]

    is (layoutHash 2 $ I.fromList [ (1, [ 'a', 'b' ]) ]) Nothing
    is (layoutHash 2 $ I.fromList [ (0, [ 'a', 'b' ]) ])
        $ Just $ reverse [ (0, 'a'), (1, 'b') ]
    is (layoutHash 4 $ I.fromList [ (1, [ 'a', 'b' ]), (2, ['c']) ])
        $ Just $ reverse [ (1, 'a'), (2, 'b'), (3, 'c') ]
    is (layoutHash 4 $ I.fromList [ (1, [ 'a', 'b' ]), (2, ['c', 'd']) ]) Nothing

    is (coalesceHash (4 :: Int) [(259, 'a'), (263, 'b')])
        $ I.fromList [ (1, [ (263, 'b'), (259, 'a') ]) ]
    is (createHashVector 4 (0 :: Int, 'O') [(259, 'a'), (263, 'b')])
        $ V.fromList [(0,'O'),(263,'b'),(259,'a'),(0,'O')]
    is (createHashVector 2 (0 :: Int, 'O') [(259, 'a'), (263, 'b')])
        $ V.fromList [(0,'O'),(263,'b'),(259,'a')]
    is (createHashVector 1 (0 :: Int, 'O') [(259, 'a'), (263, 'b')])
        $ V.fromList [(0,'O'),(263,'b'),(259,'a')]

    liftIO $ makeCDB (addBS foo (B.pack "baz")) (td ++ "/j.out")
    r2 <- liftIO $ openCDB $ td ++ "/j.out"

    v2 <- liftIO $ getBS r2 foo
    is (map B.unpack v2) [ "baz" ]
    liftIO $ closeCDB r2
    liftIO $ closeCDB r
    where foo = B.pack "foo"
