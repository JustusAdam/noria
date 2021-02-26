#!/usr/bin/env stack
-- stack --resolver=lts-14.6 script
{-# LANGUAGE OverloadedStrings, TypeFamilies, GADTs, LambdaCase,
  TypeApplications, NamedFieldPuns, RecordWildCards,
  ScopedTypeVariables, AllowAmbiguousTypes, ImplicitParams,
  MultiWayIf, ViewPatterns, TupleSections, DuplicateRecordFields,
  DeriveGeneric, FlexibleContexts #-}

import Control.Category ((>>>))
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.ST (runST)
import Control.Monad.Writer (execWriterT, tell)
import Data.Bifunctor (first, second)
import Data.Foldable (foldr', for_)
import Data.Function ((&))
import qualified Data.HashTable.Class as HT hiding (new)
import qualified Data.HashTable.ST.Linear as HT (new)
import qualified Data.IntMap as IM
import Data.Maybe (fromMaybe, mapMaybe)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Traversable (for)
import GHC.Conc (getNumProcessors)
import GHC.Generics (Generic, Rep)
import Options.Applicative
import System.Directory
import System.Environment
import System.Exit (ExitCode(ExitFailure), exitSuccess)
import System.FilePath
import System.IO (Handle, IOMode(WriteMode), hPrint, hPutStrLn, withFile, hPutStr, hPutChar)
import System.IO.Unsafe (unsafePerformIO)
import System.Process
import System.Random
import Text.Printf (hPrintf, printf)
import qualified Toml

import Debug.Trace

import Control.Exception (assert)

data Range a =
    Range
        { low :: a
        , high :: a
        }
    deriving (Read, Generic)

instance Toml.HasCodec a => Toml.HasCodec (Range a) where
    hasCodec = Toml.table Toml.genericCodec

instance Toml.HasCodec a => Toml.HasItemCodec (Range a) where
    hasItemCodec = Right Toml.genericCodec

data SumComparison =
    SumComparison
        { numSelectable :: Word
        , numGenRange :: [Range Word]
        , valueLimit :: Range Int
        , lookupRounds :: Word
        , lookupsPerRound :: Word
        , queries :: [T.Text]
        }
    deriving (Read, Generic)

instance Toml.HasCodec SumComparison where
    hasCodec = Toml.table Toml.genericCodec

data ClickstreamEvo =
    ClickstreamEvo
        { numUsers :: Word
        , eventRange :: [Range Word]
        , boundaryProb :: Float
        , numLookups :: Word
        , queries :: [T.Text]
        }
    deriving (Read, Generic)

instance Toml.HasCodec ClickstreamEvo where
    hasCodec = Toml.table Toml.genericCodec

type ParallelismRange = Range (Maybe Word)

data ClickstreamSharding =
    ClickstreamSharding
        { csBasic :: ClickstreamEvo
        , sharding :: ParallelismRange
        }
    deriving (Read, Generic)

data Separation =
    Separation
        { exclusive :: Maybe Bool
        , ops :: [T.Text]
        , dump_graph :: Maybe T.Text
        }
    deriving (Generic)

instance Toml.HasCodec Separation where
    hasCodec = Toml.table Toml.genericCodec

instance Toml.HasItemCodec Separation where
    hasItemCodec = Right Toml.genericCodec

data AvgSplitDomain =
    AvgSplitDomain
        { basic :: SumComparison
        , separations :: [Separation]
        }
    deriving (Generic)

data RedesignConf =
    RedesignConf
        { sharding :: [Int]
        , probeScale :: Float
        , noiseScale :: Float
        , noisePool :: Int
        , numUsers :: Int
        , probings :: Int
        , measureChunkSize :: Int
        }
    deriving (Generic)

instance Toml.HasCodec RedesignConf where
    hasCodec = Toml.table Toml.genericCodec

data REConf =
    REConf
        { num_probes :: Int
        , num_probings :: Int
        , num_noise_makers :: Int
        , noise_pool_size :: Int
        , shards :: Int
        , num_users :: Int
        , table_name :: T.Text
        , query_name :: T.Text
        , query_file :: T.Text
        , measure_chunk_size :: Int
        }
    deriving (Generic)

data EConf =
    EConf
        { query_file :: T.Text
        , table_file :: T.Text
        , data_file :: T.Text
        , lookup_file :: T.Text
        , sharding :: Maybe Word
        , logging :: Maybe Bool
        , separate_ops :: Maybe [T.Text]
        , separate_ops_exclusive :: Maybe Bool
        , dump_graph :: Maybe T.Text
        }
    deriving (Generic)

assertM :: Applicative m => Bool -> m ()
assertM = flip assert (pure ())

splitOn :: Char -> String -> [String]
splitOn _ "" = []
splitOn c s =
    cons
        (case break (== c) s of
             (l, s') ->
                 ( l
                 , case s' of
                       [] -> []
                       _:s'' -> splitOn c s''))
  where
    cons ~(h, t) = h : t

dataFile :: (?outputFile :: FilePath) => FilePath
dataFile = ?outputFile -<.> "data.csv"

lookupFile :: (?outputFile :: FilePath) => FilePath
lookupFile = ?outputFile -<.> "lookups.csv"

tomlDecode :: (Generic a, Toml.GenericCodec (Rep a)) => T.Text -> a
tomlDecode = either (error . show) id . Toml.decode Toml.genericCodec

runBenches ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => T.Text
    -> String
    -> IO ()
runBenches a =
    \case
        "avg" -> sumCompBench "avg" "SumCount" $ tomlDecode a
        "sum-comp" -> sumCompBench "sum-comp" "TabSum" $ tomlDecode a
        "clickstream-evo" -> clickStreamEvoBench $ tomlDecode a
        "clickstream-sharding" -> clickStreamShardingBench $ tomlDecode a
        "avg-split-domain" ->
            avgSplitDomainBench "avg-split-domain" "SumCount" $ tomlDecode a
        "redesign" -> redesign $ tomlDecode a

redesign ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => RedesignConf
    -> IO ()
redesign RedesignConf {..} = do
    compileAlgo "click_ana.ohuac" "click_ana"
    withStdOutputFile $ \h -> do
        hPutStrLn h "Noise,User,Requests,Time"
        for_ [0,1,2,3,4,5,6] $ \noise -> do
            res <-
                runBenchBin
                    REConf
                        { num_probes = 4 -- ceiling $ probeScale * fromIntegral shards
                        , num_probings = probings
                        , num_noise_makers = noise -- ceiling $ noiseScale * fromIntegral shards
                        , noise_pool_size = noisePool
                        , shards = 4
                        , num_users = numUsers
                        , table_name = "clicks"
                        , query_name = "clickstream_ana"
                        , query_file = T.pack $ takeDirectory ?outputFile </> "query.sql"
                        , measure_chunk_size = measureChunkSize
                        }
            for_ (lines res) $ \(splitOn ',' -> [usr, reqs, time]) ->
                hPrintf h "%d,%s,%s,%s\n" noise usr reqs time

withStdOutputFile :: (?outputFile :: FilePath) => (Handle -> IO a) -> IO a
withStdOutputFile = withFile ?outputFile WriteMode

avgSplitDomainBench ::
       (?genericOpts :: GenericOpts, ?outputFile :: String)
    => String
    -> String
    -> AvgSplitDomain
    -> IO ()
avgSplitDomainBench baseName tabName AvgSplitDomain {..} =
    withStdOutputFile $ \h -> do
        let writeResults = const $ hPrintf h "%s,%s,%i,%i\n"
        hPutStrLn h "Phase,Lang,Items,Time"
        configurableSumCompBench
            baseName
            tabName
            (\q ->
                 [ (basicEConf q)
                     { separate_ops = Just ops
                     , separate_ops_exclusive = exclusive
                     , dump_graph = dump_graph
                     }
                 | Separation {..} <- separations
                 ])
            writeResults
            basic

runBenchBin ::
       ( ?genericOpts :: GenericOpts
       , ?outputFile :: String
       , Generic c
       , Toml.GenericCodec (Rep c)
       )
    => c
    -> IO String
runBenchBin cfg = do
    T.writeFile confPath (Toml.encode Toml.genericCodec cfg)
    rustCommand ["run", "--bin", "features"] [confPath]
  where
    confPath = dropExtension ?outputFile ++ "-econf.toml"

basicEConf ::
       (?outputFile :: String, ?genericOpts :: GenericOpts) => String -> EConf
basicEConf qfile =
    EConf
        { query_file = T.pack qfile
        , data_file = T.pack dataFile
        , lookup_file = T.pack lookupFile
        , sharding = Nothing
        , logging = Nothing
        , separate_ops = Nothing
        , dump_graph = T.pack <$> dumpGraph ?genericOpts
        , separate_ops_exclusive = Nothing
        , table_file = T.pack $ takeDirectory qfile </> "tables.sql"
        }

clickStreamEvoBench ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => ClickstreamEvo
    -> IO ()
clickStreamEvoBench evo =
    withFile ?outputFile WriteMode $ \h -> do
        hPutStrLn h "Version,Phase,Events,Time"
        configurableClickStreamBench
            (pure . basicEConf)
            (\_ query phase Range {..} time ->
                 hPrintf
                     h
                     "%s,%s,%i,%i\n"
                     query
                     phase
                     ((high + low) `div` 2)
                     (time :: Word))
            evo

parallelismRangeToBounds :: ParallelismRange -> IO (Word, Word)
parallelismRangeToBounds Range {..} =
    verify . (low', ) <$>
    case high of
        Nothing -> fromIntegral <$> getNumProcessors
        Just set -> pure set
  where
    low' = fromMaybe 1 low
    verify t@(a, b)
        | a > b = error "Range ordering!"
        | otherwise = t

clickStreamShardingBench ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => ClickstreamSharding
    -> IO ()
clickStreamShardingBench sconf =
    withFile ?outputFile WriteMode $ \h -> do
        hPutStrLn h "Version,Shards,Phase,Events,Time"
        (plow, phigh) <-
            parallelismRangeToBounds (sharding (sconf :: ClickstreamSharding))
        configurableClickStreamBench
            (\q ->
                 [ (basicEConf q)
                     { sharding =
                           if shardNum == 0
                               then Nothing
                               else Just shardNum
                     }
                 | shardNum <- [plow .. phigh]
                 ])
            (\cfg query phase Range {..} time ->
                 hPrintf
                     h
                     "%s,%i,%s,%i,%i\n"
                     query
                     (fromMaybe 0 $ sharding (cfg :: EConf))
                     phase
                     ((high + low) `div` 2)
                     (time :: Word))
            (csBasic sconf)

configurableClickStreamBench ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => (String -> [EConf])
    -> (EConf -> T.Text -> String -> Range Word -> Word -> IO ())
    -> ClickstreamEvo
    -> IO ()
configurableClickStreamBench mkConfs writeResults eg@ClickstreamEvo {..} = do
    compileAlgo "click_ana.ohuac" "click_ana"
    for_ eventRange $ \evr -> do
        clickstreamEvoGen eg evr
        when genOnly exitSuccess
        handlerFunc <- mkHandlerFunc evr
        printf "Running for events from %i to %i\n" (low evr) (high evr)
        replicateM_ (fromIntegral repeat) $
            for_ queries $ \query -> do
                let queryFile = printf "clickstream-evo/%s.sql" query
                for_ (mkConfs queryFile) $ \cfg ->
                    runBenchBin cfg >>= mapM_ (handlerFunc cfg query) . lines
    writeFile dataFile ""
  where
    GenericOpts {..} = ?genericOpts
    imperativeClickStream dat = do
        tab <- HT.new
        forM_ dat $ \(uid, cat, _ts)
            -- Not using `ts` because in the data generates timestamps in order
         ->
            HT.mutate
                tab
                uid
                (\(fromMaybe ([], Nothing) -> (stack, counter)) ->
                     (, ()) $
                     Just $
                     case cat of
                         1 -> (stack, Just (0 :: Word))
                         2 -> (maybe id (:) counter stack, Nothing)
                         _ -> (stack, (+ 1) <$> counter))
        l <- HT.toList tab
        pure $
            IM.fromList $
            map
                (second $ \(st, _) ->
                     if null st
                         then 0.0
                         else fromIntegral (sum st) / fromIntegral (length st))
                l
    mkHandlerFunc evr
        | isVerify = do
            f <- readFile dataFile
            let dat =
                    mapMaybe
                        (\case
                             (splitOn ',' -> [uid, cat, ts]) ->
                                 Just
                                     ( read uid :: Int
                                     , read cat :: Int
                                     , read ts :: Word)
                             other ->
                                 trace ("Unparseable data: " <> other) Nothing) $
                    drop 2 $ lines f
            let expected = runST (imperativeClickStream dat)
            pure $ \_ _ ->
                \case
                    (splitOn ',' -> [read -> key, read -> val]) ->
                        let precalc = expected IM.! key :: Double
                         in printf
                                "%v, %i, %f, %f\n"
                                (show $ precalc == val)
                                key
                                precalc
                                val
                    other -> putStrLn $ "Unparseable: " <> other
        | otherwise =
            pure $ \cfg query ->
                \case
                    (splitOn ',' -> [phase, _, time]) ->
                        writeResults cfg query phase evr (read time)
                    p -> printf "Weird line: %s" p

weightedRandom ::
       forall a m. MonadIO m
    => [(Float, a)]
    -> m a
weightedRandom weights' =
    liftIO $ do
        r <- randomRIO (0.0, 1.0)
        let Left a =
                foldr'
                    (\(i, a) ->
                         (>>= \((i +) -> acc) ->
                                  if r < acc
                                      then Left a
                                      else pure acc))
                    (Right 0)
                    weights
        pure a
  where
    weights = map (first (/ sum (map fst weights'))) weights'

randomRangeIO :: Random r => Range r -> IO r
randomRangeIO Range {..} = randomRIO (low, high)

clickstreamEvoGen ::
       (?outputFile :: FilePath) => ClickstreamEvo -> Range Word -> IO ()
clickstreamEvoGen ClickstreamEvo {..} eventRange' = do
    putStrLn "Started data generation"
    if True
        then callProcess "./gen_data" ["--num-lookups", show numLookups, "--num-users", show numUsers, "--lookup-file", lookupFile, "--data-file", dataFile, "--boundary-prob", show boundaryProb, "--ev-range-low", show (low eventRange'), "--ev-range-high", show (high eventRange')
                                      ]
        else doHere
    putStrLn "Finished data generation"
  where
    doHere = do
      withFile dataFile WriteMode $ \h -> do
        hPutStrLn h "#clicks"
        hPutStrLn h "i32,i32,i32"
        for_ [0 .. numUsers] $ \i -> do
            hPutStr h (show i)
            hPutStrLn h ",2,0"
            numEvents <- randomRangeIO eventRange'
            for_ [1 .. numEvents] $ \e -> do
                assertM $ not (boundaryProb >= 0.5)
                ty <-
                    weightedRandom
                        [ (boundaryProb, 1)
                        , (boundaryProb, 2)
                        , (1.0 - 2.0 * boundaryProb, 0)
                        ]
                hPutStr h (show i)
                hPutChar h ','
                hPutStr h (show (ty :: Int))
                hPutChar h ','
                hPutStr h (show e)
      withFile lookupFile WriteMode $ \h -> do
        hPutStrLn h "#clickstream_ana"
        hPutStrLn h "i32"
        replicateM_ (fromIntegral numLookups) $ do
            n <- randomRIO (0, numUsers)
            hPrint h n

sumCompGen ::
       (?outputFile :: FilePath)
    => String
    -> SumComparison
    -> Range Word
    -> IO ()
sumCompGen tabName SumComparison {..} ngr = do
    withFile dataFile WriteMode $ \h -> do
        hPutStrLn h "#Tab"
        hPutStrLn h "i32,i32"
        for_ [0 .. numSelectable] $ \i -> hPrintf h "%i,0\n" i
        for_ [0 .. numSelectable] $ \i -> do
            toGen <- randomRangeIO ngr
            replicateM_ (fromIntegral toGen) $ do
                val <- randomRangeIO valueLimit
                hPrintf h "%i,%i\n" i val
    withFile lookupFile WriteMode $ \h -> do
        hPutStrLn h $ "#" <> tabName
        hPutStrLn h "i32"
        replicateM_ (fromIntegral lookupRounds) $
            replicateM (fromIntegral lookupsPerRound) $ do
                n <- randomRIO (0, numSelectable)
                hPutStrLn h (show n)

compileAlgo :: (?genericOpts :: GenericOpts) => FilePath -> FilePath -> IO ()
compileAlgo algoFile entrypoint =
    unless (genOnly ?genericOpts) $ do
        algoSource <- makeAbsolute algoFile
        void $
            readCreateProcess
                (proc "ohuac" $
                 [ "build"
                 , "-g"
                 , "noria-udf"
                 , algoSource
                 , entrypoint
                 , "-v"
                 , "--debug"
                 ] <>
                 ["--force" | recompile ?genericOpts])
                    {cwd = Just noriaDir}
                ""

rustCommand ::
       (?genericOpts :: GenericOpts) => [String] -> [String] -> IO String
rustCommand cargoArgs binArgs0 = do
    fenv <- buildEnv
    readCreateProcess
        (proc "cargo" (cargoArgs <> buildArgs <> binArgs)) {env = fenv}
        ""
  where
    binArgs =
        case binArgs0 of
            [] -> binArgs0
            _ -> "--" : binArgs0
    GenericOpts {..} = ?genericOpts
    buildArgs
        | noRelease = []
        | otherwise = ["--release"]
    buildEnv
        | isVerify = Just . (("VERIFY", "1") :) <$> getEnvironment
        | otherwise = pure Nothing

sumCompBench ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => String
    -> String
    -> SumComparison
    -> IO ()
sumCompBench baseName tabName sc =
    withFile ?outputFile WriteMode $ \h -> do
        let writeResults = const $ hPrintf h "%s,%s,%i,%i\n"
        hPutStrLn h "Phase,Lang,Items,Time"
        configurableSumCompBench
            baseName
            tabName
            (pure . basicEConf)
            writeResults
            sc

configurableSumCompBench ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => String
    -> String
    -> (String -> [EConf])
    -> (EConf -> String -> T.Text -> Word -> Word -> IO ())
    -> SumComparison
    -> IO ()
configurableSumCompBench queryBaseName tabName mkConf writeResults sc@SumComparison {..} = do
    compileAlgo "avg.ohuac" "avg"
    for_ numGenRange $ \n@Range {..} -> do
        let items = (low + high) `div` 2
        gen n
        handlerFunc <- mkHandlerFunc items
        for_ queries $ \query ->
            replicateM_ (fromIntegral repeat) $
            let queryFile = printf "%s/%s.sql" queryBaseName query
             in for (mkConf queryFile) $ \conf ->
                    runBenchBin conf >>= mapM_ (handlerFunc conf query) . lines
  where
    mkHandlerFunc items
        | isVerify = do
            dat <- readFile dataFile
            let expected =
                    mapMaybe
                        (\case
                             (splitOn ',' -> [(read -> k), (read -> v)]) ->
                                 Just (k, [v :: Int])
                             other ->
                                 trace ("Unparseable data: " <> other) Nothing)
                        (drop 2 $ lines dat) &
                    IM.fromListWith (<>) &
                    fmap (\v -> fromIntegral (sum v) / fromIntegral (length v))
            pure $ \_ _ ->
                \case
                    (splitOn ',' -> [read -> key, read -> val]) ->
                        let precalc = expected IM.! key :: Double
                         in printf
                                "%v,%f,%f\n"
                                (show $ precalc == val)
                                precalc
                                val
                    other -> putStrLn $ "Unparseable: " <> other
        | otherwise =
            pure $ \conf query (splitOn ',' -> [phase, _, time]) ->
                writeResults conf phase query items (read time)
    gen = sumCompGen tabName sc
    GenericOpts {..} = ?genericOpts

noriaDir :: (?genericOpts :: GenericOpts) => FilePath
noriaDir =
    unsafePerformIO $ maybe (getCurrentDirectory >>= go) pure noriaDirectory
  where
    go dir = do
        arrived <- ("noria-server" `elem`) <$> getDirectoryContents dir
        if | arrived -> pure dir
           | dir == "/" ->
               fail
                   "Expected to be in a noria subdirectory, or the directory to be provided"
           | otherwise -> go (takeDirectory dir)
    GenericOpts {..} = ?genericOpts

data Opts =
    Opts
        { expName :: FilePath
        , configPath :: Maybe FilePath
        , genericOpts :: GenericOpts
        }

data GenericOpts =
    GenericOpts
        { noRelease :: Bool
        , forceRegen :: Bool
        , genOnly :: Bool
        , repeat :: Word
        , noriaDirectory :: Maybe FilePath
        , isVerify :: Bool
        , recompile :: Bool
        , dumpGraph :: Maybe String
        }

acParser :: ParserInfo Opts
acParser = info (helper <*> p) fullDesc
  where
    p =
        Opts <$> strArgument (metavar "NAME" <> help "Experiment name") <*>
        optional
            (strOption
                 (long "config" <>
                  metavar "PATH" <>
                  help
                      "Explicitly provide the config name (also sets the path for the experiment. Otherwise the `NAME` is used)")) <*>
        goptsParser
    goptsParser =
        GenericOpts <$>
        switch
            (long "no-release" <>
             help "No not build the executable in --release mode") <*>
        switch (long "regen" <> help "Force regeneration of input data") <*>
        switch
            (long "gen-only" <> help "Only generate the input data, do not run") <*>
        option auto (long "repeat" <> value 1 <> help "Do multiple runs") <*>
        optional
            (strOption $
             long "noria-dir" <> help "Explicitly point to the noria dir") <*>
        switch (long "verify" <> help "Verify output instead of measuring") <*>
        switch (long "force" <> help "Force recompilation") <*>
        optional
            (strOption $ long "dump-graph" <> help "Dump the query in this file")

allTargets =
    [ "avg"
    , "sum-comp"
    , "clickstream-evo"
    , "clickstream-sharding"
    --, "avg-split-domain"
    ]

main :: IO ()
main =
    execParser acParser >>= \Opts {..} ->
        let ?genericOpts = genericOpts
         in let mkConfigPath =
                    (</> if isVerify genericOpts
                             then "verify.toml"
                             else "conf.toml")
                go0 n = go (mkConfigPath n) n
             in case configPath of
                    Just p -> go p expName
                    Nothing
                        | expName == "all" -> mapM_ go0 allTargets
                        | otherwise -> go0 expName
  where
    go config expName = do
        cfgStr <- T.readFile config
        --for_ (zip [(0 ::Int)..] cfgs) $ \(i, cfg) ->
        let ?outputFile = config -<.> "csv"
         in runBenches cfgStr expName
