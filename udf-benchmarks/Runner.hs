#!stack runhaskell
{-# LANGUAGE OverloadedStrings, TypeFamilies, GADTs, LambdaCase,
  TypeApplications, NamedFieldPuns, RecordWildCards,
  ScopedTypeVariables, AllowAmbiguousTypes, ImplicitParams,
  MultiWayIf, ViewPatterns, TupleSections, DuplicateRecordFields,
  DeriveGeneric #-}

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
import GHC.Generics (Generic)
import Options.Applicative
import System.Directory
import System.Environment
import System.Exit (ExitCode(ExitFailure), exitSuccess)
import System.FilePath
import System.IO (IOMode(WriteMode), hPrint, hPutStrLn, withFile)
import System.IO.Unsafe (unsafePerformIO)
import System.Process
import System.Random
import Text.Printf (hPrintf, printf)
import GHC.Conc (getNumProcessors)
import qualified Toml

import Debug.Trace

import Control.Exception (assert)

data Bench
    = SumComparison' SumComparison
    | SumCount' SumComparison
    | ClickstreamEvo' ClickstreamEvo
    | ClickstreamSharding' ClickstreamSharding
    deriving (Read)

data SumComparison = SumComparison
    { numSelectable :: Word
    , numGenRange :: [(Word, Word)]
    , valueLimit :: (Int, Int)
    , lookupRounds :: Word
    , lookupsPerRound :: Word
    , queries :: [String]
    } deriving (Read)

data ClickstreamEvo = ClickstreamEvo
    { numUsers :: Word
    , eventRange :: [(Word, Word)]
    , boundaryProb :: Float
    , numLookups :: Word
    , queries :: [String]
    } deriving (Read)

data ParallelismRange = ParallelismRange
    { partitionsLow :: Word
    , partitionsHigh :: Maybe Word
    } deriving Read

data ClickstreamSharding = ClickstreamSharding
    { csBasic :: ClickstreamEvo
    , sharding :: ParallelismRange
    } deriving (Read)

data EConf = EConf
    { query_file :: T.Text
    , data_file :: T.Text
    , lookup_file :: T.Text
    , sharding :: Maybe Word
    , logging :: Maybe Bool
    } deriving (Generic)

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

runBenches ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath) => Bench -> IO ()
runBenches =
    \case
        SumCount' a -> sumCompBench "avg" "SumCount" a
        SumComparison' a -> sumCompBench "sum-comp" "TabSum" a
        ClickstreamEvo' a -> clickStreamEvoBench a
        ClickstreamSharding' a -> clickStreamShardingBench a

runBenchBin ::
       (?genericOpts :: GenericOpts, ?outputFile :: String)
    => EConf
    -> IO String
runBenchBin cfg = do
    T.writeFile confPath (Toml.encode Toml.genericCodec cfg)
    rustCommand ["run", "--bin", "features"] [confPath]
  where
    confPath = dropExtension ?outputFile ++ "-econf.toml"

basicEConf :: (?outputFile :: String) => String -> EConf
basicEConf qfile =
    EConf
        { query_file = T.pack qfile
        , data_file = T.pack dataFile
        , lookup_file = T.pack lookupFile
        , sharding = Nothing
        , logging = Nothing
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
            (\ _ query phase (lo, hi) time ->
                 hPrintf
                     h
                     "%s,%s,%i,%i\n"
                     query
                     phase
                     ((hi + lo) `div` 2)
                     (time :: Word))
            evo

parallelismRangeToBounds :: ParallelismRange -> IO (Word, Word)
parallelismRangeToBounds r =
    verify <$>
    case partitionsHigh r of
        Nothing -> (partitionsLow r, ) . fromIntegral <$> getNumProcessors
        Just set -> pure (partitionsLow r, set)
  where
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
                  [(basicEConf q)
                      { sharding =
                            if shardNum == 0
                                then Nothing
                                else Just shardNum
                      }
                  | shardNum <- [plow..phigh]])
            (\cfg query phase (lo, hi) time ->
                  hPrintf
                      h
                      "%s,%i,%s,%i,%i\n"
                      query
                      (fromMaybe 0 $ sharding (cfg :: EConf))
                      phase
                      ((hi + lo) `div` 2)
                      (time :: Word))
            (csBasic sconf)

configurableClickStreamBench ::
       (?genericOpts :: GenericOpts, ?outputFile :: FilePath)
    => (String -> [ EConf ])
    ->  (EConf -> String -> String -> (Word, Word) -> Word -> IO ())
    -> ClickstreamEvo
    -> IO ()
configurableClickStreamBench mkConfs writeResults eg@ClickstreamEvo {..} = do
    compileAlgo "click_ana.ohuac" "click_ana"
    for_ eventRange $ \evr -> do
        clickstreamEvoGen eg evr
        when genOnly exitSuccess
        handlerFunc <- mkHandlerFunc evr
        replicateM_ (fromIntegral repeat) $
            for_ queries $ \query -> do
                let queryFile = printf "clickstream-evo/%s.sql" query
                for_ (mkConfs queryFile) $ \cfg ->
                    runBenchBin cfg >>= mapM_ (handlerFunc cfg query) . lines
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
            pure $ \cfg query (splitOn ',' -> [phase, _, time]) ->
                writeResults cfg query phase evr (read time)

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

clickstreamEvoGen ::
       (?outputFile :: FilePath) => ClickstreamEvo -> (Word, Word) -> IO ()
clickstreamEvoGen ClickstreamEvo {..} eventRange' = do
    withFile dataFile WriteMode $ \h -> do
        hPutStrLn h "#clicks"
        hPutStrLn h "i32,i32,i32"
        for_ [0 .. numUsers] $ \i -> do
            hPrintf h "%i,2,0\n" i
            numEvents <- randomRIO eventRange'
            for_ [1 .. numEvents] $ \e -> do
                assertM $ not (boundaryProb >= 0.5)
                ty <-
                    weightedRandom
                        [ (boundaryProb, 1)
                        , (boundaryProb, 2)
                        , (1.0 - 2.0 * boundaryProb, 0)
                        ]
                hPrintf h "%i,%i,%i\n" i (ty :: Int) e
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
    -> (Word, Word)
    -> IO ()
sumCompGen tabName SumComparison {..} ngr = do
    withFile dataFile WriteMode $ \h -> do
        hPutStrLn h "#Tab"
        hPutStrLn h "i32,i32"
        for_ [0 .. numSelectable] $ \i -> hPrintf h "%i,0\n" i
        for_ [0 .. numSelectable] $ \i -> do
            toGen <- randomRIO ngr
            replicateM_ (fromIntegral toGen) $ do
                val <- randomRIO valueLimit
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
sumCompBench queryBaseName tabName sc@SumComparison {..} = do
    compileAlgo "avg.ohuac" "avg"
    withFile ?outputFile WriteMode $ \h -> do
        hPutStrLn h "Phase,Lang,Items,Time"
        for_ numGenRange $ \n@(lo, hi) -> do
            let items = (lo + hi) `div` 2
            gen n
            handlerFunc <- mkHandlerFunc items
            for_ queries $ \query ->
                replicateM_ (fromIntegral repeat) $
                let queryFile = printf "%s/%s.sql" queryBaseName query
                 in runBenchBin (basicEConf queryFile) >>=
                    mapM_ (handlerFunc h query) . lines
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
            pure $ \h query (splitOn ',' -> [phase, _, time]) ->
                hPrintf h "%s,%s,%i,%i\n" phase query items (read time :: Word)
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

data Opts = Opts
    { config :: FilePath
    , genericOpts :: GenericOpts
    }

data GenericOpts = GenericOpts
    { noRelease :: Bool
    , forceRegen :: Bool
    , genOnly :: Bool
    , repeat :: Word
    , noriaDirectory :: Maybe FilePath
    , isVerify :: Bool
    , recompile :: Bool
    }

acParser :: ParserInfo Opts
acParser = info (helper <*> p) fullDesc
  where
    p =
        Opts <$>
        strArgument (metavar "CONFIG" <> help "Path to experiment config") <*>
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
        switch (long "force" <> help "Force recompilation")

main :: IO ()
main =
    execParser acParser >>= \Opts {..} -> do
        let ?genericOpts = genericOpts
        cfg <- read <$> readFile config
        --for_ (zip [(0 ::Int)..] cfgs) $ \(i, cfg) ->
        let ?outputFile = config -<.> "csv"
         in runBenches cfg
