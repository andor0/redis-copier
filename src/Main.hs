{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Options.Applicative
import Data.Monoid ((<>))
import qualified Database.Redis.Redis as R
import qualified Data.ByteString as B

data ConsoleArgs = ConsoleArgs
  { src_host    :: String
  , src_port    :: Int
  , src_db      :: Int
  , dst_host    :: String
  , dst_port    :: Int
  , dst_db      :: Int
  }

args :: Parser ConsoleArgs
args = ConsoleArgs
  <$> strOption
    ( long "src-host"
   <> metavar "HOST"
   <> help "src redis host" )
  <*> option auto
    ( long "src-port"
   <> metavar "PORT"
   <> help "src redis port" )
  <*> option auto
    ( long "src-db"
   <> metavar "DB"
   <> help "src redis db" )
  <*> strOption
    ( long "dst-host"
   <> metavar "HOST"
   <> help "dst redis host" )
  <*> option auto
    ( long "dst-port"
   <> metavar "PORT"
   <> help "dst redis port" )
  <*> option auto
    ( long "dst-db"
   <> metavar "DB"
   <> help "dst redis db" )

main :: IO ()
main = execParser opts >>= connect >>= copy >>= finish
  where
    opts = info (args <**> helper)
      ( fullDesc
     <> header "redis_copier" )

connect :: ConsoleArgs -> IO (R.Redis, R.Redis, Int)
connect (ConsoleArgs src_host src_port src_db dst_host dst_port dst_db) = do
  src_redis <- R.connect src_host (show src_port)
  R.select src_redis src_db
  dst_redis <- R.connect dst_host (show dst_port)
  R.select dst_redis dst_db
  return (src_redis, dst_redis, 0)

copy :: (R.Redis, R.Redis, Int) -> IO (R.Redis, R.Redis, Int)
copy (src_redis, dst_redis, !number_of_copies) = do
  resultRandomKey <- R.randomKey src_redis :: IO (R.Reply B.ByteString)
  case resultRandomKey of
    R.RBulk Nothing -> return (src_redis, dst_redis, number_of_copies)
    R.RBulk (Just key) -> do
      resultKeyType <- R.getType src_redis key
      case resultKeyType of
        R.RTString -> do
          resultValue <- R.get src_redis key :: IO (R.Reply B.ByteString)
          case resultValue of
            R.RBulk Nothing -> copy (src_redis, dst_redis, number_of_copies)
            R.RBulk (Just value) -> do
              resultTTL <- R.ttl src_redis key
              case resultTTL of
                R.RInt ttl
                  | ttl == -1 -> R.set dst_redis key value >> R.del src_redis key >> copy (src_redis, dst_redis, number_of_copies + 1)
                  | ttl == -2 -> copy (src_redis, dst_redis, number_of_copies)
                  | otherwise -> R.setEx dst_redis key ttl value >> R.del src_redis key >> copy (src_redis, dst_redis, number_of_copies + 1)
        _ -> copy (src_redis, dst_redis, number_of_copies)

finish :: (R.Redis, R.Redis, Int) -> IO ()
finish (src_redis, dst_redis, number_of_copies) = do
  putStrLn $ "number of copies: " ++ show number_of_copies
  R.disconnect src_redis
  R.disconnect dst_redis
