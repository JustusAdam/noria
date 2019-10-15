#!stack runhaskell
{-# LANGUAGE OverloadedStrings, TypeFamilies, GADTs, LambdaCase,
  TypeApplications, NamedFieldPuns, RecordWildCards,
  ScopedTypeVariables, AllowAmbiguousTypes, ImplicitParams,
  MultiWayIf, ViewPatterns #-}

import Control.Category ((>>>))
import Control.Monad
import Control.Monad.Writer (execWriterT, tell)
import Data.Bifunctor (first, second)
import Data.Foldable (foldr', for_)
import Data.Traversable (for)
import Options.Applicative
import System.Directory
import System.FilePath
import System.IO (IOMode(WriteMode), hPutStrLn, withFile, hPrint)
import System.IO.Unsafe (unsafePerformIO)
import System.Process
import System.Random
import System.Exit (exitSuccess)
import Text.Printf (hPrintf, printf)
import Control.Monad.IO.Class

import Control.Exception (assert)

type Results = [(String, [Word])]

data Bench
    = SumComparison { numSelectable :: Word
                    , numGenRange :: [(Word, Word)]
                    , valueLimit :: (Int, Int)
                    , lookupRounds :: Word
                    , lookupsPerRound :: Word
                    , queries :: [String] }
    | SumCount { numSelectable :: Word
               , numGenRange :: [(Word, Word)]
               , valueLimit :: (Int, Int)
               , lookupRounds :: Word
               , lookupsPerRound :: Word
               , queries :: [String] }
    | ClickstreamEvo { numUsers :: Word
                     , eventRange :: [(Word, Word)]
                     , boundaryProb :: Float
                     , numLookups :: Word
                     , queries :: [String] }
    deriving (Read, Show)

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

runBenches :: (?genericOpts :: GenericOpts) => FilePath -> Bench -> IO ()
runBenches outputFile =
    \case
        SumCount {..} -> do
            ex <- doesFileExist fname
    --when (forceRegen || genOnly || not ex) $ gen
            unless genOnly runBench
            where runBench =
                      sumCompBench
                          "avg"
                          "SumCount"
                          numGenRange
                          queries
                          outputFile
                          fname
                          lfname
                          valueLimit
                          lookupRounds
                          lookupsPerRound
                          numSelectable
        SumComparison {..} -> do
            ex <- doesFileExist fname
    --when (forceRegen || genOnly || not ex) $ gen
            unless genOnly runBench
            where runBench =
                      sumCompBench
                          "sum-comp"
                          "TabSum"
                          numGenRange
                          queries
                          outputFile
                          fname
                          lfname
                          valueLimit
                          lookupRounds
                          lookupsPerRound
                          numSelectable
        ClickstreamEvo {..} -> do
            compileAlgo "click_ana.ohuac" "click_ana"
            withFile outputFile WriteMode $ \h -> do
                hPutStrLn h "Version,Phase,Events,Time"
                for_ eventRange $ \evr@(lo, hi) -> do
                    clickstreamEvoGen
                        fname
                        lfname
                        numUsers
                        evr
                        boundaryProb
                        numLookups
                    when genOnly exitSuccess
                    replicateM_ (fromIntegral repeat) $
                        for_ queries $ \query -> do
                            let queryFile =
                                    printf "clickstream-evo/%s.sql" query
                            ls <-
                                readProcess
                                    "cargo"
                                    [ "run"
                                    , "--bin"
                                    , "features"
                                    , "--"
                                    , queryFile
                                    , fname
                                    , lfname
                                    ]
                                    ""
                            for_ (lines ls) $ \(splitOn ',' -> [phase, _, time]) ->
                                hPrintf
                                    h
                                    "%s,%s,%i,%i\n"
                                    query
                                    phase
                                    ((hi + lo) `div` 2)
                                    (read time :: Word)
  where
    fname = outputFile -<.> "data.csv"
    lfname = outputFile -<.> "lookups.csv"
    GenericOpts {..} = ?genericOpts

weightedRandom :: forall a m. MonadIO m => [(Float, a)] -> m a
weightedRandom weights' = liftIO $ do
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
       FilePath -> FilePath -> Word -> (Word, Word) -> Float -> Word -> IO ()
clickstreamEvoGen fname lfname numUsers eventRange boundaryProb numLookups = do
    withFile fname WriteMode $ \h -> do
        hPutStrLn h "#clicks"
        hPutStrLn h "i32,i32,i32"
        for_ [0 .. numUsers] $ \i -> do
            hPrintf h "%i,2,0" i
            numEvents <- randomRIO eventRange
            for_ [1 .. numEvents] $ \e -> do
                assertM $ not (boundaryProb >= 0.5)
                ty <-
                    weightedRandom
                        [ (boundaryProb, 0)
                        , (boundaryProb, 1)
                        , (1.0 - 2.0 * boundaryProb, 2)
                        ]
                hPrintf h "%i,%i,%i" i e (ty :: Int)
    withFile lfname WriteMode $ \h -> do
        hPutStrLn h "#clickstream_ana"
        hPutStrLn h "i32"
        replicateM_ (fromIntegral numLookups) $ do
            n <- randomRIO (0, numUsers)
            hPrint h n

sumCompGen ::
       String
    -> FilePath
    -> FilePath
    -> (Int, Int)
    -> Word
    -> Word
    -> Word
    -> (Word, Word)
    -> IO ()
sumCompGen tabName fname lfname valueLimit lookupRounds lookupsPerRound numSelectable numGenRange = do
    withFile fname WriteMode $ \h -> do
        hPutStrLn h "#Tab"
        hPutStrLn h "i32,i32"
        for_ [0 .. numSelectable] $ \i -> hPrintf h "%i,0\n" i
        for_ [0 .. numSelectable] $ \i -> do
            toGen <- randomRIO valueLimit
            replicateM_ (fromIntegral toGen) $ do
                val <- randomRIO valueLimit
                hPrintf h "%i,%i\n" i val
    withFile lfname WriteMode $ \h -> do
        hPutStrLn h $ "#" <> tabName
        hPutStrLn h "i32"
        replicateM_ (fromIntegral lookupRounds) $
            replicateM (fromIntegral lookupsPerRound) $ do
                n <- randomRIO (0, numSelectable)
                hPutStrLn h (show n)

compileAlgo :: (?genericOpts :: GenericOpts) => FilePath -> FilePath -> IO ()
compileAlgo algoFile entrypoint = unless (genOnly ?genericOpts) $ do
    algoSource <- makeAbsolute algoFile
    void $
        readCreateProcess
            (proc
                 "ohuac"
                 [ "build"
                 , "-g"
                 , "noria-udf"
                 , algoSource
                 , entrypoint
                 , "-v"
                 , "--debug"
                 ])
                {cwd = Just noriaDir}
            ""

buildRust :: (?genericOpts :: GenericOpts) => IO ()
buildRust = callProcess "cargo" buildArgs
  where
    buildArgs
        | noRelease ?genericOpts = ["build"]
        | otherwise = ["build", "--release"]

sumCompBench ::
       (?genericOpts :: GenericOpts)
    => String
    -> String
    -> [(Word, Word)]
    -> [String]
    -> FilePath
    -> FilePath
    -> FilePath
    -> (Int, Int)
    -> Word
    -> Word
    -> Word
    -> IO ()
sumCompBench queryBaseName tabName numGenRange queries outputFile fname lfname valueLimit lookupRounds lookupsPerRound numSelectable = do
    compileAlgo "avg.ohuac" "avg"
    withFile outputFile WriteMode $ \h -> do
        hPutStrLn h "Phase,Lang,Items,Time"
        for_ numGenRange $ \n@(lo, hi) -> do
            let items = (lo + hi) `div` 2
            gen n
            for_ queries $ \query ->
                replicateM_ (fromIntegral repeat) $
                let queryFile = printf "%s/%s.sql" queryBaseName query
                 in readProcess
                        "cargo"
                        [ "run"
                        , "--bin"
                        , "features"
                        , "--"
                        , queryFile
                        , fname
                        , lfname
                        ]
                        "" >>=
                    mapM_
                        (\(splitOn ',' -> [phase, _, time]) ->
                             hPrintf
                                 h
                                 "%s,%s,%i,%i\n"
                                 phase
                                 query
                                 items
                                 (read time :: Word)) .
                    lines
  where
    gen =
        sumCompGen
            tabName
            fname
            lfname
            valueLimit
            lookupRounds
            lookupsPerRound
            numSelectable
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
             long "noria-dir" <> help "Explicitly point to the noria dir")

main :: IO ()
main =
    execParser acParser >>= \Opts {..} -> do
        let ?genericOpts = genericOpts
        cfg <- read <$> readFile config
        --for_ (zip [(0 ::Int)..] cfgs) $ \(i, cfg) ->
        runBenches (config -<.> "csv") cfg
