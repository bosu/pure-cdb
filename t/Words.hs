module Main (main) where

import Database.PureCDB
import qualified Data.ByteString.Char8 as B
import System.Unix.Directory (withTemporaryDirectory)

main :: IO ()
main = withTemporaryDirectory "/tmp/pure_cdb_XXXXXXX" $ \td -> do
    ls <- B.lines `fmap` B.readFile "/usr/share/dict/words"
    makeCDB (mapM_ (\s -> addBS s s) ls) (td ++ "/j.out")
