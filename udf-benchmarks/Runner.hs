#!stack runhaskell
{-# LANGUAGE OverloadedStrings, TypeFamilies, GADTs, LambdaCase,
  TypeApplications, NamedFieldPuns, RecordWildCards,
  ScopedTypeVariables, AllowAmbiguousTypes, ImplicitParams,
  MultiWayIf #-}

import Control.Monad (replicateM, replicateM_, unless, when)
import Data.Bifunctor (second)
import Data.Foldable (for_)
import Data.Hashable
import Data.List (intercalate)
import qualified Data.Map as Map
import Data.Traversable (for)
import Options.Applicative
import System.Directory
import System.FilePath
import System.IO (IOMode(WriteMode), hPutStrLn, withFile)
import System.IO.Unsafe (unsafePerformIO)
import System.Process
import System.Random
import Text.Printf (hPrintf, printf)

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
    | ClickstreamEvo 
        { numUsers :: Word 
        , eventRange :: (Word, Word)
        , boundaryProb :: Float
        , numLookups :: Word
        , queries :: [String]
        }
    deriving (Read, Show)

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
                          "sum-count"
                          numGenRange
                          queries
                          outputFile
                          fname
                          lfname
                          repeat
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
                          numGenRange
                          queries
                          outputFile
                          fname
                          lfname
                          repeat
                          valueLimit
                          lookupRounds
                          lookupsPerRound
                          numSelectable
        b@ClickstreamEvo{} ->  
            clickstreamEvoGen fname lname b
            compileAlgo "click_ana.ohuac"
            buildRust
            withFile outputFile WriteMode $ \h ->
                replicateM repeat $ do
                    for_ queries $ \query -> do
                        let queryFile = printf "clickstream-evo/%s.sql" query
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
                        for_ (lines ls) $ \line ->
                            let l =  splitOn ',' lr
                            hPrintf h "%s,%s,%i\n" query (l !! 0) (read $ l !! 2 :: Word)
  where
    fname = outputFile -<.> "data"
    lfname = outputFile -<.> "lookups"
    GenericOpts {..} = ?genericOpts

weightedRandom :: [(Float, a)] -> IO a
weightedRandom weights' = do
    r <- randomRIO (0.0,1.0)
    fromLeft $ foldr' (\(i,a) -> (>>= \acc -> let acc' = i + acc in if r < acc' then Left a else pure acc')) (Right 0) weights
  where
    weights = map (first (/ sum (map fst weights'))) weights

clickstreamEvoGen :: FilePath -> FilePath -> Bench -> IO ()
clickstreamEvoGen fname lfname ClickstreamEvo {..} = do
    withFile fname WriteMode $ \h -> do
        hPutStrLn h "#clicks"
        hPutStrLn h "i32,i32,i64"
        for_ [0..numUsers] $ \i -> do
            hPrintf h "%i,2,0\n" i
            numEvents <- randomRIO eventRange
            for_ [1..numEvents] $ \e -> do
                assertM $ not (boundaryProb >= 0.5)
                ty <- weightedRandom [(boundaryProb, 0), (boundaryProb, 1), (1.0-2.0*boundaryProb,2)]
                hPrintf h "%i,%i,%i\n" i e ty 
    withFile lfname WriteMode $ \h -> do
        hPutStrLn h "#clickstream_ana"
        hPutStrLn h "i32"
        replicateM (fromIntegral numLookups) $ do
            n <- randomRIO (0,numUsers)
            hPutStrLn h (show n)


sumCompGen ::
       FilePath
    -> FilePath
    -> (Int, Int)
    -> Word
    -> Word
    -> Word
    -> (Word, Word)
    -> IO ()
sumCompGen fname lfname valueLimit lookupRounds lookupsPerRound numSelectable numGenRange = do
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
        hPutStrLn h "#TabSum"
        hPutStrLn h "i32"
        replicateM_ (fromIntegral lookupRounds) $
            replicateM (fromIntegral lookupsPerRound) $ do
                n <- randomRIO (0,numSelectable)
                hPutStrLn h (show n)

compileAlgo :: 
       (?genericOpts :: GenericOpts)
        => FilePath -> IO ()
compileAlgo algoFile = do
    algoSource <- makeAbsolute algoFile
    readCreateProcess
        (proc
             "ohuac"
             [ "build"
             , "-g"
             , "noria-udf"
             , algoSource
             , "sum_count"
             , "-v"
             , "--debug"
             ])
            {cwd = Just noriaDir}
        ""

buildRust :: 
       (?genericOpts :: GenericOpts)
       => IO ()
buildRust = 
    callProcess "cargo" buildArgs
  where buildArgs
            | noRelease = ["build"]
            | otherwise = ["build", "--release"]

sumCompBench ::
       (?genericOpts :: GenericOpts)
    => String
    -> [(Word, Word)]
    -> [String]
    -> FilePath
    -> FilePath
    -> FilePath
    -> Word
    -> (Int, Int)
    -> Word
    -> Word
    -> Word
    -> IO ()
sumCompBench queryBaseName numGenRange queries outputFile fname lfname repeat valueLimit lookupRounds lookupsPerRound numSelectable = do
    compileAlgo "sum_count.ohuac"
    buildRust
    res <-
        for numGenRange $ \n -> do
            gen n
            for queries $ \query ->
                replicateM (fromIntegral repeat) $ do
                    let queryFile = printf "%s-%s.sql" queryBaseName query
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
                    pure
                        [ (l !! 0, read $ l !! 2 :: Int)
                        | lr <- lines ls
                        , let l = splitOn ',' lr
                        ]
    writeFile
        outputFile
        (unlines $
         "Phase,Lang,Items,Time" :
         [ printf "%s,%s,%i,%i" phase lang items time
         | (items, qs) <- zip (map ((+ 30) . fst) numGenRange) res
         , (lang, runs) <- zip queries qs
         , dat <- runs
         , (phase, time) <- dat
         ])
  where
    gen =
        sumCompGen
            fname
            lfname
            valueLimit
            lookupRounds
            lookupsPerRound
            numSelectable
    GenericOpts {..} = ?genericOpts

noriaDir :: (?genericOpts :: GenericOpts) => FilePath
noriaDir =
    unsafePerformIO $ do
        case noriaDirectory of
            Just dir -> pure dir
            Nothing ->
                let go dir = do
                        arrived <-
                            ("noria-server" `elem`) <$> getDirectoryContents dir
                        if | arrived -> pure dir
                           | dir == "/" ->
                               fail
                                   "Expected to be in a noria subdirectory, or the directory to be provided"
                           | otherwise -> go (takeDirectory dir)
                 in getCurrentDirectory >>= go
  where
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
