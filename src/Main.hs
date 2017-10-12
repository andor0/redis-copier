{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Options.Applicative
import Data.Monoid ((<>))
import qualified Database.Redis as R

data ConsoleArgs = ConsoleArgs
  { src_host    :: String
  , src_port    :: Integer
  , src_db      :: Integer
  , dst_host    :: String
  , dst_port    :: Integer
  , dst_db      :: Integer
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

connect :: ConsoleArgs -> IO (R.Connection, R.Connection, Int)
connect (ConsoleArgs src_host src_port src_db dst_host dst_port dst_db) = do
  src_redis <- R.connect $ R.defaultConnectInfo { R.connectHost = src_host
                                                , R.connectPort = R.PortNumber . fromInteger $ src_port
                                                , R.connectDatabase = src_db }
  dst_redis <- R.connect $ R.defaultConnectInfo { R.connectHost = dst_host
                                                , R.connectPort = R.PortNumber . fromInteger $ dst_port
                                                , R.connectDatabase = dst_db }
  return (src_redis, dst_redis, 0)

copy :: (R.Connection, R.Connection, Int) -> IO (R.Connection, R.Connection, Int)
copy (src_redis, dst_redis, !number_of_copies) = dump src_redis >>= restore src_redis dst_redis number_of_copies
  where
    dump src_redis = R.runRedis src_redis $ do
      resultRandomKey <- R.randomkey
      case resultRandomKey of
        Right (Just key) -> do
          resultDump <- R.dump key
          case resultDump of
            Right dump -> do
              resultTTL <- R.ttl key
              case resultTTL of
                Right ttl | ttl == -2 -> return (Just key, Nothing, Nothing)
                          | ttl == -1 -> return (Just key, Just dump, Just 0)
                          | otherwise -> return (Just key, Just dump, Just $ ttl * 1000)
                _ -> return (Just key, Nothing, Nothing)
            _ -> return (Just key, Nothing, Nothing)
        _ -> return (Nothing, Nothing, Nothing)

    restore src_redis dst_redis number_of_copies (Nothing, _, _) = return (src_redis, dst_redis, number_of_copies)
    restore src_redis dst_redis number_of_copies (_, Nothing, _) = copy(src_redis, dst_redis, number_of_copies)
    restore src_redis dst_redis number_of_copies (Just key, Just dump, Just ttl) = do
      R.runRedis dst_redis $ R.restore key ttl dump
      R.runRedis src_redis $ R.del [key]
      copy(src_redis, dst_redis, number_of_copies + 1)

finish :: (R.Connection, R.Connection, Int) -> IO ()
finish (src_redis, dst_redis, number_of_copies) = putStrLn $ "number of copies: " ++ show number_of_copies
