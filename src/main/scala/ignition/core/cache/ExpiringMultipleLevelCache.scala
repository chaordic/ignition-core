package ignition.core.cache

import java.util.concurrent.TimeUnit

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import ignition.core.utils.DateUtils._
import ignition.core.utils.FutureUtils._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spray.caching.ValueMagnet

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ExpiringMultipleLevelCache {
  case class TimestampedValue[V](date: DateTime, value: V) {
    def hasExpired(ttl: FiniteDuration, now: DateTime): Boolean = {
      date.plus(ttl.toMillis).isBefore(now)
    }
  }

  trait GenericCache[V] { cache =>
    // Keep compatible with Spray Cache
    def apply(key: String) = new Keyed(key)

    class Keyed(key: String) {
      /**
        * Returns either the cached Future for the key or evaluates the given call-by-name argument
        * which produces either a value instance of type `V` or a `Future[V]`.
        */
      def apply(magnet: ⇒ ValueMagnet[V])(implicit ec: ExecutionContext): Future[V] =
        cache.apply(key, () ⇒ try magnet.future catch { case NonFatal(e) ⇒ Future.failed(e) })

      /**
        * Returns either the cached Future for the key or evaluates the given function which
        * should lead to eventual completion of the promise.
        */
      def apply[U](f: Promise[V] ⇒ U)(implicit ec: ExecutionContext): Future[V] =
        cache.apply(key, () ⇒ { val p = Promise[V](); f(p); p.future })
    }

    def apply(key: String, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext): Future[V]
  }

  trait LocalCache[V] extends GenericCache[V] {
    def get(key: Any): Option[Future[V]]
    def set(key: Any, value: V): Unit
  }

  trait RemoteWritableCache[V] {
    def set(key: String, value: V)(implicit ec: ExecutionContext): Future[Unit]
    def setLock(key: String, ttl: FiniteDuration)(implicit ec: ExecutionContext): Future[Boolean]
  }

  trait RemoteReadableCache[V] {
    def get(key: String)(implicit ec: ExecutionContext): Future[Option[V]]
  }

  trait RemoteCacheRW[V] extends RemoteReadableCache[V] with RemoteWritableCache[V]

  trait ReporterCallback {
    def onCompletedWithFailure(key: String, e: Throwable, elapsedTime: FiniteDuration): Unit
    def onGeneratedWithSuccess(key: String, elapsedTime: FiniteDuration): Unit
    def onCompletedWithSuccess(key: String, elapsedTime: FiniteDuration): Unit
    def onGeneratedWithFailure(key: String, e: Throwable, elapsedTime: FiniteDuration): Unit
    def onCacheMissNothingFound(key: String, elapsedTime: FiniteDuration): Unit
    def onCacheMissButFoundExpiredLocal(key: String, elapsedTime: FiniteDuration): Unit
    def onCacheMissButFoundExpiredRemote(key: String, elapsedTime: FiniteDuration): Unit
    def onRemoteCacheHit(key: String, elapsedTime: FiniteDuration): Unit
    def onLocalCacheHit(key: String, elapsedTime: FiniteDuration): Unit
    def onUnexpectedBehaviour(key: String, elapsedTime: FiniteDuration): Unit
    def onStillTryingToLockOrGet(key: String, elapsedTime: FiniteDuration): Unit
    def onSuccessfullyRemoteSetValue(key: String, elapsedTime: FiniteDuration): Unit
    def onRemoteCacheHitAfterGenerating(key: String, elapsedTime: FiniteDuration): Unit
    def onErrorGeneratingValue(key: String, eLocal: Throwable, elapsedTime: FiniteDuration): Unit
    def onLocalError(key: String, e: Throwable, elapsedTime: FiniteDuration): Unit
    def onRemoteError(key: String, t: Throwable, elapsedTime: FiniteDuration): Unit
    def onRemoteGiveUp(key: String, elapsedTime: FiniteDuration): Unit
  }

  object NoOpReporter extends ReporterCallback {
    override def onCacheMissNothingFound(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onUnexpectedBehaviour(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onSuccessfullyRemoteSetValue(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onRemoteError(key: String, t: Throwable, elapsedTime: FiniteDuration): Unit = {}
    override def onRemoteGiveUp(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onLocalError(key: String, e: Throwable, elapsedTime: FiniteDuration): Unit = {}
    override def onErrorGeneratingValue(key: String, eLocal: Throwable, elapsedTime: FiniteDuration): Unit = {}
    override def onRemoteCacheHitAfterGenerating(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onCacheMissButFoundExpiredRemote(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onStillTryingToLockOrGet(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onLocalCacheHit(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onRemoteCacheHit(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onCacheMissButFoundExpiredLocal(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onCompletedWithFailure(key: String, e: Throwable, elapsedTime: FiniteDuration): Unit = {}
    override def onCompletedWithSuccess(key: String, elapsedTime: FiniteDuration): Unit = {}
    override def onGeneratedWithFailure(key: String, e: Throwable, elapsedTime: FiniteDuration): Unit = {}
    override def onGeneratedWithSuccess(key: String, elapsedTime: FiniteDuration): Unit = {}

  }
}


import ignition.core.cache.ExpiringMultipleLevelCache._


case class ExpiringMultipleLevelCache[V](ttl: FiniteDuration,
                                         remoteRW: Option[RemoteCacheRW[TimestampedValue[V]]] = None,
                                         remoteLockTTL: FiniteDuration = 5.seconds,
                                         reporter: ExpiringMultipleLevelCache.ReporterCallback = ExpiringMultipleLevelCache.NoOpReporter,
                                         maxErrorsToRetryOnRemote: Int = 5) extends GenericCache[V] {

  private val logger = LoggerFactory.getLogger(getClass)

  private val tempUpdate = new ConcurrentLinkedHashMap.Builder[Any, Future[TimestampedValue[V]]]
    .maximumWeightedCapacity(Long.MaxValue)
    .build()

  protected def now = DateTime.now

  private def timestamp(v: V) = TimestampedValue(now, v)

  private def elapsedTime(startNanoTime: Long) = FiniteDuration(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS)

  private def remoteLockKey(key: Any) = s"$key-emlc-lock"


  // The idea is simple, have two caches: remote and local
  // with values that will eventually expire but still be left on the cache
  // while a new value is asynchronously being calculated/retrieved
  override def apply(key: String, genValue: () => Future[V])(implicit ec: ExecutionContext): Future[V] = {
    // The local cache is always the first try. We'll only look the remote if the local value is missing or has expired
    val startTime = System.nanoTime()
    val result =
        // No local, let's try remote
        remoteRW.get.get(key).asTry().flatMap {
          case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
            // Remote is good, set locally and return it
            reporter.onRemoteCacheHit(key, elapsedTime(startTime))
            Future.successful(remoteValue.value)
          case Success(Some(expiredRemote)) =>
            // Expired remote, return the it, async update
            reporter.onCacheMissButFoundExpiredRemote(key, elapsedTime(startTime))
            tryGenerateAndSet(key, genValue, startTime).map(_.value)
            Future.successful(expiredRemote.value)
          case Success(None) =>
            // No good remote, sync generate
            reporter.onCacheMissNothingFound(key, elapsedTime(startTime))
            tryGenerateAndSet(key, genValue, startTime).map(_.value)
          case Failure(e) =>
            reporter.onRemoteError(key, e, elapsedTime(startTime))
            logger.warn(s"ExpiringMultipleLevelCache.apply, key: $key expired local value and no remote configured", e)
            tryGenerateAndSet(key, genValue, startTime).map(_.value)
        }
    result.onComplete {
      case Success(_) =>
        reporter.onCompletedWithSuccess(key, elapsedTime(startTime))
      case Failure(e) =>
        reporter.onCompletedWithFailure(key, e, elapsedTime(startTime))
    }
    result
  }

  // Note: this method may return a failed future, but it will never cache it
  // Our main purpose here is to avoid multiple local calls to generate new promises/futures in parallel,
  // so we use this Map keep everyone in sync
  // This is similar to how spray cache works
  private def tryGenerateAndSet(key: String, genValue: () => Future[V], nanoStartTime: Long)(implicit ec: ExecutionContext): Future[TimestampedValue[V]] = {
    val promise = Promise[TimestampedValue[V]]()
    tempUpdate.putIfAbsent(key, promise.future) match {
      case null =>
        canonicalValueGenerator(key, genValue, nanoStartTime).onComplete {
          case Success(v) if !v.hasExpired(ttl, now) =>
            reporter.onGeneratedWithSuccess(key, elapsedTime(nanoStartTime))
            promise.trySuccess(v)
            tempUpdate.remove(key)
          case Success(v) =>
            // Have we generated/got an expired value!?
            reporter.onUnexpectedBehaviour(key, elapsedTime(nanoStartTime))
            logger.warn(s"tryGenerateAndSet, key $key: unexpectedly generated/got an expired value: $v")
            promise.trySuccess(v)
            tempUpdate.remove(key)
          case Failure(e) =>
            // We don't save failures to cache
            // There is no need to log here, canonicalValueGenerator will log everything already
            reporter.onGeneratedWithFailure(key, e, elapsedTime(nanoStartTime))
            promise.tryFailure(e)
            tempUpdate.remove(key)
        }
        promise.future
      case fTrying =>
        // If someone call us while a future is running, we return the running future
        fTrying
    }
  }

  // This can be called by multiple instances/hosts simultaneously but in the end
  // only the one that wins the race will create the final value that will be set in
  // the remote cache and read by the other instances
  // Unless of course there is some error getting stuff from remote cache
  // in which case the locally generated value may be returned to avoid further delays
  protected def canonicalValueGenerator(key: String, genValue: () => Future[V], nanoStartTime: Long)(implicit ec: ExecutionContext) = {
    val fGeneratedValue = Try { genValue().map(timestamp) }.asFutureTry()
    val finalValue: Future[TimestampedValue[V]] = fGeneratedValue.flatMap {
      case Success(generatedValue) =>
        // Successfully generated value, try to set it in the remote writable cache
        remoteRW match {
          // No remote cache available, just return this value to be set on local cache
          case None =>
            Future.successful(generatedValue)
          case Some(remote) =>
            remoteSetOrGet(key, generatedValue, remote, nanoStartTime)
        }
      case Failure(eLocal) =>
        // We failed to generate the value ourselves, our hope is if someone else successfully did it in the meantime
        reporter.onErrorGeneratingValue(key, eLocal, elapsedTime(nanoStartTime))
        remoteRW match {
          case None =>
            // There are no remote RW caches
            logger.error(s"canonicalValueGenerator, key $key: failed to generate value and no remote cache configured", eLocal)
            Future.failed(eLocal)
          case Some(remote) =>
            remoteGetNonExpiredValue(key, remote, nanoStartTime).asTry().flatMap {
              case Success(v) =>
                logger.warn(s"canonicalValueGenerator, key $key: failed to generate value but got one from remote", eLocal)
                Future.successful(v)
              case Failure(eRemote) =>
                // The real error is the eLocal, return it
                logger.error(s"canonicalValueGenerator, key $key: failed to generate value and failed to get remote", eLocal)
                Future.failed(eLocal)
            }
        }
    }
    finalValue
  }

  // Auxiliary method, only makes sense to be used by canonicalValueGenerator
  private def remoteGetNonExpiredValue(key: String,
                                       remote: RemoteCacheRW[TimestampedValue[V]],
                                       nanoStartTime: Long,
                                       currentRetry: Int = 0)(implicit ec: ExecutionContext): Future[TimestampedValue[V]] = {
    remote.get(key).asTry().flatMap {
      case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
        Future.successful(remoteValue)
      case Success(_) =>
        Future.failed(new Exception("No good value found on remote"))
      case Failure(e) =>
        if (currentRetry >= maxErrorsToRetryOnRemote) {
          reporter.onRemoteGiveUp(key, elapsedTime(nanoStartTime))
          logger.error(s"remoteGetWithRetryOnError, key $key: returning calculated value because we got more than $maxErrorsToRetryOnRemote errors", e)
          Future.failed(e)
        } else {
          reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
          logger.warn(s"remoteGetWithRetryOnError, key $key: got error trying to get value, retry $currentRetry of $maxErrorsToRetryOnRemote", e)
          // Retry
          remoteGetNonExpiredValue(key, remote, nanoStartTime, currentRetry = currentRetry + 1)
        }
    }
  }

  // This methods tries to guarantee that everyone that calls it in
  // a given moment will be left with the same value in the end
  private def remoteSetOrGet(key: String,
                             calculatedValue: TimestampedValue[V],
                             remote: RemoteCacheRW[TimestampedValue[V]],
                             nanoStartTime: Long,
                             currentRetry: Int = 0)(implicit ec: ExecutionContext): Future[TimestampedValue[V]] = {
    if (currentRetry > maxErrorsToRetryOnRemote) {
      // Use our calculated value as it's the best we can do
      reporter.onRemoteGiveUp(key, elapsedTime(nanoStartTime))
      logger.error(s"remoteSetOrGet, key $key: returning calculated value because we got more than $maxErrorsToRetryOnRemote errors")
      Future.successful(calculatedValue)
    } else {
      remote.setLock(remoteLockKey(key), remoteLockTTL).asTry().flatMap {
        case Success(true) =>
          logger.info(s"remoteSetOrGet got lock for key $key")
          // Lock acquired, get the current value and replace it
          remote.get(key).asTry().flatMap {
            case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
              // Current value is good, just return it
              reporter.onRemoteCacheHitAfterGenerating(key, elapsedTime(nanoStartTime))
              logger.info(s"remoteSetOrGet got lock for $key but found already a good value on remote")
              Future.successful(remoteValue)
            case Success(_) =>
              // The remote value is missing or has expired. This is what we were expecting
              // We have the lock to replace this value. Our calculated value will be the canonical one!
              remote.set(key, calculatedValue).asTry().flatMap {
                case Success(_) =>
                  // Flawless victory!
                  reporter.onSuccessfullyRemoteSetValue(key, elapsedTime(nanoStartTime))
                  logger.info(s"remoteSetOrGet successfully set key $key while under lock")
                  Future.successful(calculatedValue)
                case Failure(e) =>
                  reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
                  logger.warn(s"remoteSetOrGet, key $key: got error setting the value, retry $currentRetry of $maxErrorsToRetryOnRemote", e)
                  // Retry failure
                  remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry + 1)
              }
            case Failure(e) =>
              reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
              logger.warn(s"remoteSetOrGet, key $key: got error getting remote value with lock, retry $currentRetry of $maxErrorsToRetryOnRemote", e)
              // Retry failure
              remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry + 1)
          }
        case Success(false) =>
          // Someone got the lock, let's take a look at the value
          remote.get(key).asTry().flatMap {
            case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
              // Current value is good, just return it
              reporter.onRemoteCacheHitAfterGenerating(key, elapsedTime(nanoStartTime))
              Future.successful(remoteValue)
            case Success(_) =>
              // The value is missing or has expired
              // Let's start from scratch because we need to be able to set or get a good value
              // Note: do not increment retry because this isn't an error
              reporter.onStillTryingToLockOrGet(key, elapsedTime(nanoStartTime))
              logger.info(s"remoteSetOrGet couldn't lock key $key and didn't found good value on remote")
              remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry)
            case Failure(e) =>
              reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
              logger.warn(s"remoteSetOrGet, key $key: got error getting remote value without lock, retry $currentRetry of $maxErrorsToRetryOnRemote", e)
              // Retry
              remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry + 1)
          }
        case Failure(e) =>
          // Retry failure
          reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
          logger.warn(s"remoteSetOrGet, key $key: got error trying to set lock, retry $currentRetry of $maxErrorsToRetryOnRemote", e)
          remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry + 1)
      }
    }
  }
}