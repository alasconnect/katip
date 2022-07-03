{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

-- | This is an internal module. No guarantees are made in this module
-- about API stability.
module Katip.Scribes.ElasticSearch.Internal where

-------------------------------------------------------------------------------
import Control.Applicative as A
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM.TBMQueue
import qualified Control.Exception.Safe as EX
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.STM
import Control.Retry
  ( RetryPolicy,
    exponentialBackoff,
    limitRetries,
    recovering,
  )
import Data.Aeson
import Data.ByteString.Lazy (ByteString)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Monoid as Monoid
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time
import Data.Time.Calendar.WeekDate
import Data.Typeable as Typeable
import Data.UUID
import qualified Data.UUID.V4 as UUID4
import qualified Database.Bloodhound                  as V7

-------------------------------------------------------------------------------
import Katip.Core
import Katip.Scribes.ElasticSearch.Annotations
import Network.HTTP.Client
import Network.HTTP.Types.Status
import Text.Printf (printf)

-------------------------------------------------------------------------------

-- | EsScribeCfg now carries a type variable for the version of
-- ElasticSearch it targets, either 'ESV1' or 'ESV5'. You can use
-- 'defaultEsScribeCfgV1' and 'defaultESScribeCfgV5' for a good
-- starting point depending on the ES version you have.
data EsScribeCfg v = EsScribeCfg
  { -- | Retry policy when there are errors sending logs to the server
    essRetryPolicy :: RetryPolicy,
    -- | Maximum size of the bounded log queue
    essQueueSize :: EsQueueSize,
    -- | Worker pool size limit for sending data to the
    essPoolSize :: EsPoolSize,
    -- | Different payload items coexist in the "data" attribute in
    -- ES. It is possible for different payloads to have different
    -- types for the same key, e.g. an "id" key that is sometimes a
    -- number and sometimes a string. If you're having ES do dynamic
    -- mapping, the first log item will set the type and any that
    -- don't conform will be *discarded*. If you set this to True,
    -- keys will recursively be appended with their ES core
    -- type. e.g. "id" would become "id::l" and "id::s"
    -- automatically, so they won't conflict. When this library
    -- exposes a querying API, we will try to make deserialization and
    -- querying transparently remove the type annotations if this is
    -- enabled.
    essAnnotateTypes :: Bool,
    -- | This will be the IndexSettings type from the appropriate
    -- bloodhound module, either @Database.V1.Bloodhound@ or
    -- @Database.V5.Bloodhound@
    essIndexSettings :: IndexSettings v,
    essIndexSharding :: IndexShardingPolicy
  }
  deriving (Typeable)

-- | Reasonable defaults for a config:
--
--     * defaultManagerSettings
--
--     * exponential backoff with 25ms base delay up to 5 retries, for a total cumulative delay of 775ms
--
--     * Queue size of 1000
--
--     * Pool size of 2
--
--     * Annotate types set to False
--
--     * DailyIndexSharding
defaultEsScribeCfg' :: ESVersion v => proxy v -> EsScribeCfg v
defaultEsScribeCfg' prx =
  EsScribeCfg
    { essRetryPolicy = exponentialBackoff 25000 Monoid.<> limitRetries 5,
      essQueueSize = EsQueueSize 1000,
      essPoolSize = EsPoolSize 2,
      essAnnotateTypes = False,
      essIndexSettings = defaultIndexSettings prx,
      essIndexSharding = DailyIndexSharding
    }

-------------------------------------------------------------------------------

-- | Alias of 'defaultEsScribeCfgV7' to minimize API
-- breakage. Previous versions of katip-elasticsearch only supported
-- ES version 1 and defaulted to it.
defaultEsScribeCfg :: EsScribeCfg ESV7
defaultEsScribeCfg = defaultEsScribeCfgV7

-------------------------------------------------------------------------------

-------------------------------------------------------------------------------

-- | EsScribeCfg that will use ElasticSearch V7
defaultEsScribeCfgV7 :: EsScribeCfg ESV7
defaultEsScribeCfgV7 = defaultEsScribeCfg' (Typeable.Proxy :: Typeable.Proxy ESV7)

-------------------------------------------------------------------------------

-- | How should katip store your log data?
--
-- * NoIndexSharding will store all logs in one index name. This is
-- the simplest option but is not advised in production. In practice,
-- the index will grow very large and will get slower to
-- search. Deleting records based on some sort of retention period is
-- also extremely slow.
--
-- * MonthlyIndexSharding, DailyIndexSharding, HourlyIndexSharding,
-- EveryMinuteIndexSharding will generate indexes based on the time of
-- the log. Index name is treated as a prefix. So if your index name
-- is @foo@ and DailySharding is used, logs will be stored in
-- @foo-2016-2-25@, @foo-2016-2-26@ and so on. Index templating will
-- be used to set up mappings automatically. Deletes based on date are
-- very fast and queries can be restricted to date ranges for better
-- performance. Queries against all dates should use @foo-*@ as an
-- index name. Note that index aliasing's glob feature is not suitable
-- for these date ranges as it matches index names as they are
-- declared, so new dates will be excluded. DailyIndexSharding is a
-- reasonable choice. Changing index sharding strategies is not
-- advisable.
--
-- * CustomSharding: supply your own function that decomposes an item
-- into its index name hierarchy which will be appended to the index
-- name. So for instance if your function return ["arbitrary",
-- "prefix"], the index will be @foo-arbitrary-prefix@ and the index
-- template will be set to match @foo-*@. In general, you want to use
-- segments of increasing granularity (like year, month, day for
-- dates). This makes it easier to address groups of indexes
-- (e.g. @foo-2016-*@).
data IndexShardingPolicy
  = NoIndexSharding
  | MonthlyIndexSharding
  | -- | A special case of daily which shards to sunday
    WeeklyIndexSharding
  | DailyIndexSharding
  | HourlyIndexSharding
  | EveryMinuteIndexSharding
  | CustomIndexSharding (forall a. Item a -> [IndexNameSegment])

instance Show IndexShardingPolicy where
  show NoIndexSharding = "NoIndexSharding"
  show MonthlyIndexSharding = "MonthlyIndexSharding"
  show WeeklyIndexSharding = "WeeklyIndexSharding"
  show DailyIndexSharding = "DailyIndexSharding"
  show HourlyIndexSharding = "HourlyIndexSharding"
  show EveryMinuteIndexSharding = "EveryMinuteIndexSharding"
  show (CustomIndexSharding _) = "CustomIndexSharding λ"

-------------------------------------------------------------------------------
newtype IndexNameSegment = IndexNameSegment
  { indexNameSegment :: Text
  }
  deriving (Show, Eq, Ord)

-------------------------------------------------------------------------------
shardPolicySegs :: IndexShardingPolicy -> Item a -> [IndexNameSegment]
shardPolicySegs NoIndexSharding _ = []
shardPolicySegs MonthlyIndexSharding Item {..} = [sis y, sis m]
  where
    (y, m, _) = toGregorian (utctDay _itemTime)
shardPolicySegs WeeklyIndexSharding Item {..} = [sis y, sis m, sis d]
  where
    (y, m, d) = toGregorian (roundToSunday (utctDay _itemTime))
shardPolicySegs DailyIndexSharding Item {..} = [sis y, sis m, sis d]
  where
    (y, m, d) = toGregorian (utctDay _itemTime)
shardPolicySegs HourlyIndexSharding Item {..} = [sis y, sis m, sis d, sis h]
  where
    (y, m, d) = toGregorian (utctDay _itemTime)
    (h, _) = splitTime (utctDayTime _itemTime)
shardPolicySegs EveryMinuteIndexSharding Item {..} = [sis y, sis m, sis d, sis h, sis mn]
  where
    (y, m, d) = toGregorian (utctDay _itemTime)
    (h, mn) = splitTime (utctDayTime _itemTime)
shardPolicySegs (CustomIndexSharding f) i = f i

-------------------------------------------------------------------------------

-- | If the given day is sunday, returns the input, otherwise returns
-- the previous sunday
roundToSunday :: Day -> Day
roundToSunday d
  | dow == 7 = d
  | w > 1 = fromWeekDate y (w - 1) 7
  | otherwise = fromWeekDate (y - 1) 53 7
  where
    (y, w, dow) = toWeekDate d

-------------------------------------------------------------------------------
chooseIxn :: ESVersion v => proxy v -> IndexName v -> IndexShardingPolicy -> Item a -> IndexName v
chooseIxn prx ixn p i =
  toIndexName prx (T.intercalate "-" ((fromIndexName prx ixn) : segs))
  where
    segs = indexNameSegment A.<$> shardPolicySegs p i

-------------------------------------------------------------------------------
sis :: Integral a => a -> IndexNameSegment
sis = IndexNameSegment . T.pack . fmt
  where
    fmt = printf "%02d" . toInteger

-------------------------------------------------------------------------------
splitTime :: DiffTime -> (Int, Int)
splitTime t = asMins `divMod` 60
  where
    asMins = floor t `div` 60

-------------------------------------------------------------------------------
data EsScribeSetupError
  = CouldNotCreateIndex !(Response ByteString)
  | CouldNotUpdateIndexSettings !(Response ByteString)
  | CouldNotCreateMapping !(Response ByteString)
  | CouldNotPutTemplate !(Response ByteString)
  deriving (Typeable, Show)

instance Exception EsScribeSetupError

-------------------------------------------------------------------------------

-- | The Any field tagged with a @v@ corresponds to the type of the
-- same name in the corresponding @bloodhound@ module. For instance,
-- if you are configuring for ElasticSearch version 1, import
-- @Database.V1.Bloodhound@ and @BHEnv v@ will refer to @BHEnv@ from
-- that module, @IndexName v@ will repsond to @IndexName@ from that
-- module, etc.
mkEsScribe ::
  forall v.
  ( ESVersion v,
    MonadIO (BH v IO)
#if defined(__GLASGOW_HASKELL__) && __GLASGOW_HASKELL__ < 800
                 , Functor (BH v IO)
#endif
  ) =>
  EsScribeCfg v ->
  BHEnv v ->
  -- | Treated as a prefix if index sharding is enabled
  IndexName v ->
  PermitFunc ->
  Verbosity ->
  IO Scribe
mkEsScribe cfg@EsScribeCfg {..} env ix permit verb = do
  q <- newTBMQueueIO $ unEsQueueSize essQueueSize
  endSig <- newEmptyMVar

  runBH prx env $ do
    if shardingEnabled
      then do
        -- create or update
        res <- putTemplate prx tpl tplName
        unless (statusIsSuccessful (responseStatus res)) $
          liftIO $ EX.throwIO (CouldNotPutTemplate res)
      else do
        ixExists <- indexExists prx ix
        if ixExists
          then do
            res <- updateIndexSettings prx (toUpdatabaleIndexSettings prx essIndexSettings) ix
            unless (statusIsSuccessful (responseStatus res)) $
              liftIO $ EX.throwIO (CouldNotUpdateIndexSettings res)
          else do
            r1 <- createIndex prx essIndexSettings ix
            unless (statusIsSuccessful (responseStatus r1)) $
              liftIO $ EX.throwIO (CouldNotCreateIndex r1)
            r2 <- putMapping prx ix base
            unless (statusIsSuccessful (responseStatus r2)) $
              liftIO $ EX.throwIO (CouldNotCreateMapping r2)

  workers <-
    replicateM (unEsPoolSize essPoolSize) $
      async $
        startWorker cfg env q

  _ <- async $ do
    takeMVar endSig
    atomically $ closeTBMQueue q
    mapM_ waitCatch workers
    putMVar endSig ()

  let finalizer = putMVar endSig () >> takeMVar endSig
  return (Scribe (logger q) finalizer permit)
  where
    logger :: forall a. LogItem a => TBMQueue (IndexName v, Value) -> Item a -> IO ()
    logger q i =
      void $ atomically $ tryWriteTBMQueue q (chooseIxn prx ix essIndexSharding i, itemJson' i)
    prx :: Typeable.Proxy v
    prx = Typeable.Proxy
    tplName = toTemplateName prx ixn
    shardingEnabled = case essIndexSharding of
      NoIndexSharding -> False
      _ -> True
    tpl = toIndexTemplate prx [(toIndexPattern prx (ixn <> "-*"))] (Just essIndexSettings) (toJSON base)
    base = baseMapping prx
    ixn = fromIndexName prx ix
    itemJson' :: LogItem a => Item a -> Value
    itemJson' i
      | essAnnotateTypes = itemJson verb (TypeAnnotated <$> i)
      | otherwise = itemJson verb i

-------------------------------------------------------------------------------
baseMapping :: ESVersion v => proxy v -> Value
baseMapping prx =
   object ["properties" .= object prs]
  where
    prs =
      [ unanalyzedString "thread",
        unanalyzedString "sev",
        unanalyzedString "pid",
        -- ns is frequently fulltext searched
        analyzedString "ns",
        -- we want message to be fulltext searchable
        analyzedString "msg",
        "loc" .= locType,
        unanalyzedString "host",
        unanalyzedString "env",
        "at" .= dateType,
        unanalyzedString "app"
      ]
    unanalyzedString k = k .= unanalyzedStringSpec prx
    analyzedString k = k .= analyzedStringSpec prx
    locType = object ["properties" .= object locPairs]
    locPairs =
      [ unanalyzedString "loc_pkg",
        unanalyzedString "loc_mod",
        unanalyzedString "loc_ln",
        unanalyzedString "loc_fn",
        unanalyzedString "loc_col"
      ]
    dateType =
      object
        [ "format" .= esDateFormat,
          "type" .= String "date"
        ]

-------------------------------------------------------------------------------

-- | Handle both old-style aeson and picosecond-level precision
esDateFormat :: Text
esDateFormat = "yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss.SSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZ"

-------------------------------------------------------------------------------
mkDocId :: ESVersion v => proxy v -> IO (DocId v)
mkDocId prx = (toDocId prx . T.decodeUtf8 . toASCIIBytes) `fmap` UUID4.nextRandom

-------------------------------------------------------------------------------
newtype EsQueueSize = EsQueueSize
  { unEsQueueSize :: Int
  }
  deriving (Show, Eq, Ord)

instance Bounded EsQueueSize where
  minBound = EsQueueSize 1
  maxBound = EsQueueSize maxBound

mkEsQueueSize :: Int -> Maybe EsQueueSize
mkEsQueueSize = mkNonZero EsQueueSize

-------------------------------------------------------------------------------
newtype EsPoolSize = EsPoolSize
  { unEsPoolSize :: Int
  }
  deriving (Show, Eq, Ord)

instance Bounded EsPoolSize where
  minBound = EsPoolSize 1
  maxBound = EsPoolSize maxBound

mkEsPoolSize :: Int -> Maybe EsPoolSize
mkEsPoolSize = mkNonZero EsPoolSize

-------------------------------------------------------------------------------
mkNonZero :: (Int -> a) -> Int -> Maybe a
mkNonZero ctor n
  | n > 0 = Just $ ctor n
  | otherwise = Nothing

-------------------------------------------------------------------------------
startWorker ::
  forall v.
  (ESVersion v) =>
  EsScribeCfg v ->
  BHEnv v ->
  TBMQueue (IndexName v, Value) ->
  IO ()
startWorker EsScribeCfg {..} env q = go
  where
    go = do
      popped <- atomically $ readTBMQueue q
      case popped of
        Just (ixn, v) -> do
          sendLog ixn v `EX.catchAny` eat
          go
        Nothing -> return ()
    prx :: Typeable.Proxy v
    prx = Typeable.Proxy
    sendLog :: IndexName v -> Value -> IO ()
    sendLog ixn v = void $
      recovering essRetryPolicy [handler] $
        const $ do
          did <- mkDocId prx
          res <- runBH prx env $ indexDocument prx ixn (defaultIndexDocumentSettings prx) v did
          return res
    eat _ = return ()
    handler _ = Handler $ \e ->
      case fromException e of
        Just (_ :: EX.SomeAsyncException) -> return False
        _ -> return True

-------------------------------------------------------------------------------
-- We are spanning multiple versions of ES which use completely
-- separate types and APIs, but the subset we use is the same for both
-- versions. This will be kept up to date with bloodhound's supported
-- versions and should be minimally visible to the end user.
class ESVersion v where
  -- Types
  type BHEnv v
  type IndexSettings v
  defaultIndexSettings :: proxy v -> IndexSettings v
  type UpdatableIndexSetting v
  type IndexName v
  toIndexName :: proxy v -> Text -> IndexName v
  fromIndexName :: proxy v -> IndexName v -> Text
  type DocId v
  toDocId :: proxy v -> Text -> DocId v
  type BH v :: (* -> *) -> * -> *
  runBH :: proxy v -> BHEnv v -> BH v m a -> m a
  type TemplateName v
  toTemplateName :: proxy v -> Text -> TemplateName v
  type IndexPattern v
  toIndexPattern :: proxy v -> Text -> IndexPattern v
  type IndexTemplate v
  toIndexTemplate :: proxy v -> [IndexPattern v] -> Maybe (IndexSettings v) -> Value -> IndexTemplate v
  type IndexDocumentSettings v
  defaultIndexDocumentSettings :: proxy v -> IndexDocumentSettings v

  toUpdatabaleIndexSettings :: proxy v -> IndexSettings v -> NonEmpty (UpdatableIndexSetting v)

  -- Operations
  -- We're deciding on IO here, but it isn't necessary
  indexExists :: proxy v -> IndexName v -> BH v IO Bool
  indexDocument :: ToJSON doc => proxy v -> IndexName v -> IndexDocumentSettings v -> doc -> DocId v -> BH v IO (Response ByteString)
  createIndex :: proxy v -> IndexSettings v -> IndexName v -> BH v IO (Response ByteString)
  updateIndexSettings :: proxy v -> NonEmpty (UpdatableIndexSetting v) -> IndexName v -> BH v IO (Response ByteString)
  putTemplate :: proxy v -> IndexTemplate v -> TemplateName v -> BH v IO (Response ByteString)
  putMapping :: (ToJSON a) => proxy v -> IndexName v -> a -> BH v IO (Response ByteString)

  -- In ES 5 and beyond, "string" was deprecated in favor of text for
  -- fulltext and keyword for unanalyzed tokens
  unanalyzedStringSpec :: proxy v -> Value
  analyzedStringSpec :: proxy v -> Value

data ESV7 = ESV7

instance ESVersion ESV7 where
  type BHEnv ESV7 = V7.BHEnv
  type IndexSettings ESV7 = V7.IndexSettings
  type UpdatableIndexSetting ESV7 = V7.UpdatableIndexSetting
  defaultIndexSettings _ = V7.defaultIndexSettings
  type IndexName ESV7 = V7.IndexName
  toIndexName _ = V7.IndexName
  fromIndexName _ (V7.IndexName x) = x
  type DocId ESV7 = V7.DocId
  toDocId _ = V7.DocId
  type BH ESV7 = V7.BH
  runBH _ = V7.runBH
  type TemplateName ESV7 = V7.TemplateName
  toTemplateName _ = V7.TemplateName
  type IndexPattern ESV7 = V7.IndexPattern
  toIndexPattern _ = V7.IndexPattern
  type IndexTemplate ESV7 = V7.IndexTemplate
  toIndexTemplate _ = V7.IndexTemplate
  type IndexDocumentSettings ESV7 = V7.IndexDocumentSettings
  toUpdatabaleIndexSettings _ s =
    (V7.NumberOfReplicas (V7.indexReplicas s)) :| []
  defaultIndexDocumentSettings _ = V7.defaultIndexDocumentSettings
  indexExists _ = V7.indexExists
  indexDocument _ = V7.indexDocument
  createIndex _ = V7.createIndex
  updateIndexSettings _ = V7.updateIndexSettings
  putTemplate _ = V7.putTemplate
  putMapping _ = V7.putMapping
  unanalyzedStringSpec _ =
    object
      [ "type" .= String "keyword"
      ]
  analyzedStringSpec _ =
    object
      [ "type" .= String "text"
      ]
