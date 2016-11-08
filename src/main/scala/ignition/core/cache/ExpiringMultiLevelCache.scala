package ignition.core.cache

import java.util.concurrent.TimeUnit

import akka.actor.Scheduler
import akka.pattern.after
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import ignition.core.utils.DateUtils._
import ignition.core.utils.FutureUtils._
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.slf4j.LoggerFactory
import spray.caching.ValueMagnet

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ExpiringMultiLevelCache {
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
      def apply(magnet: ⇒ ValueMagnet[V])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[V] =
        cache.apply(key, () ⇒ try magnet.future catch { case NonFatal(e) ⇒ Future.failed(e) })

      /**
        * Returns either the cached Future for the key or evaluates the given function which
        * should lead to eventual completion of the promise.
        */
      def apply[U](f: Promise[V] ⇒ U)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[V] =
        cache.apply(key, () ⇒ { val p = Promise[V](); f(p); p.future })
    }

    def apply(key: String, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[V]
    def set(key: String, value: V)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[Unit]
  }

  trait LocalCache[V] {
    def get(key: Any): Option[Future[V]]
    def set(key: Any, value: V): Unit
  }

  trait RemoteWritableCache[V] {
    def set(key: String, value: V)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[Unit]
    def replace(key: String, oldValue: Option[V], newValue: V)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[Boolean]
  }

  trait RemoteReadableCache[V] {
    def get(key: String)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[Option[V]]
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
    def onSanityLocalValueCheckFailedResult(key: String, result: String, elapsedTime: FiniteDuration): Unit
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
    override def onSanityLocalValueCheckFailedResult(key: String, result: String, elapsedTime: FiniteDuration): Unit = {}
  }
}


import ignition.core.cache.ExpiringMultiLevelCache._


case class ExpiringMultiLevelCache[V](ttl: FiniteDuration,
                                      localCache: Option[LocalCache[TimestampedValue[V]]],
                                      remoteRW: Option[RemoteCacheRW[TimestampedValue[V]]] = None,
                                      reporter: ExpiringMultiLevelCache.ReporterCallback = ExpiringMultiLevelCache.NoOpReporter,
                                      maxErrorsToRetryOnRemote: Int = 5,
                                      backoffOnError: FiniteDuration = 50.milliseconds,
                                      sanityLocalValueCheck: Boolean = false) extends GenericCache[V] {

  private val logger = LoggerFactory.getLogger(getClass)

  private val tempUpdate = new ConcurrentLinkedHashMap.Builder[Any, Future[TimestampedValue[V]]]
    .maximumWeightedCapacity(Long.MaxValue)
    .build()

  protected def now = DateTime.now.withZone(DateTimeZone.UTC)

  private def timestamp(v: V) = TimestampedValue(now, v)

  private def elapsedTime(startNanoTime: Long) = FiniteDuration(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS)

  // The idea is simple, have two caches: remote and local
  // with values that will eventually expire but still be left on the cache
  // while a new value is asynchronously being calculated/retrieved
  override def apply(key: String, genValue: () => Future[V])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[V] = {
    // The local cache is always the first try. We'll only look the remote if the local value is missing or has expired
    val startTime = System.nanoTime()
    val result = localCache.flatMap(_.get(key).map(_.asTry())) match {
      case Some(future) =>
        future.flatMap {
          case Success(localValue) if !localValue.hasExpired(ttl, now) =>
            // We have locally a good value, just return it
            reporter.onLocalCacheHit(key, elapsedTime(startTime))
            // But if we're paranoid, let's check if the local value is consistent with remote
            if (sanityLocalValueCheck)
              remoteRW.map(remote => sanityLocalValueCheck(key, localValue, remote, genValue, startTime)).getOrElse(Future.successful(localValue.value))
            else
              Future.successful(localValue.value)
          case Success(expiredLocalValue) if remoteRW.nonEmpty =>
            // We have locally an expired value, but we can check a remote cache for better value
            remoteRW.get.get(key).asTry().flatMap {
              case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
                // Remote is good, set locally and return it
                reporter.onRemoteCacheHit(key, elapsedTime(startTime))
                localCache.foreach(_.set(key, remoteValue))
                Future.successful(remoteValue.value)
              case Success(Some(expiredRemote)) =>
                // Expired local and expired remote, return the most recent of them, async update both
                reporter.onCacheMissButFoundExpiredRemote(key, elapsedTime(startTime))
                tryGenerateAndSet(key, genValue, startTime)
                val mostRecent = Set(expiredLocalValue, expiredRemote).maxBy(_.date)
                Future.successful(mostRecent.value)
              case Success(None) =>
                // No remote found, return local, async update both
                reporter.onCacheMissButFoundExpiredLocal(key, elapsedTime(startTime))
                tryGenerateAndSet(key, genValue, startTime)
                Future.successful(expiredLocalValue.value)
              case Failure(e) =>
                reporter.onRemoteError(key, e, elapsedTime(startTime))
                logger.warn(s"apply, key: $key expired local value and failed to get remote", e)
                tryGenerateAndSet(key, genValue, startTime)
                Future.successful(expiredLocalValue.value)
            }
          case Success(expiredLocalValue) if remoteRW.isEmpty =>
            // There is no remote cache configured, we'are on our own
            // Return expired value and try to generate a new one for the future
            reporter.onCacheMissButFoundExpiredLocal(key, elapsedTime(startTime))
            tryGenerateAndSet(key, genValue, startTime)
            Future.successful(expiredLocalValue.value)
          case Failure(e) =>
            // This is almost impossible to happen because it's local and we don't save failed values
            reporter.onLocalError(key, e, elapsedTime(startTime))
            logger.warn(s"apply, key: $key got a failed future from cache!? This is almost impossible!", e)
            tryGenerateAndSet(key, genValue, startTime).map(_.value)
        }
      case None if remoteRW.nonEmpty =>
        // No local, let's try remote
        remoteRW.get.get(key).asTry().flatMap {
          case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
            // Remote is good, set locally and return it
            reporter.onRemoteCacheHit(key, elapsedTime(startTime))
            localCache.foreach(_.set(key, remoteValue))
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
            logger.warn(s"apply, key: $key expired local value and remote error", e)
            tryGenerateAndSet(key, genValue, startTime).map(_.value)
        }
      case None if remoteRW.isEmpty =>
        // No local and no remote to look, just generate it
        // The caller will need to wait for the value generation
        reporter.onCacheMissNothingFound(key, elapsedTime(startTime))
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

  // This should be used carefully because it will overwrite the remote value without
  // any lock, which may cause a desynchronization between the local and remote cache on other instances
  // Note that if any tryGenerateAndSet is in progress, this will wait until it's finished before setting local/remote
  override def set(key: String, value: V)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[Unit] = {
    logger.info(s"set, key $key: got a call to overwrite local and remote values")
    val startTime = System.nanoTime()
    val promise = Promise[TimestampedValue[V]]()
    val future = promise.future
    def doIt() = {
      val tValue = timestamp(value)
      localCache.foreach(_.set(key, tValue))
      val result = remoteRW.map(remote => remoteOverwrite(key, tValue, remote, startTime)).getOrElse(Future.successful(tValue))
      promise.completeWith(result)
      tempUpdate.remove(key, future)
    }
    tempUpdate.put(key, future) match {
      case null =>
        doIt()
        future.map(_ => ())
      case fTrying =>
        fTrying.onComplete { case _ => doIt() }
        future.map(_ => ())
    }
  }

  private def sanityLocalValueCheck(key: String, localValue: TimestampedValue[V], remote: RemoteCacheRW[TimestampedValue[V]], genValue: () => Future[V], startTime: Long)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[V] = {
    remote.get(key).asTry().flatMap {
      case Success(Some(remoteValue)) if remoteValue == localValue =>
        // Remote is the same as local, return any of them
        Future.successful(remoteValue.value)
      case Success(Some(remoteValue)) =>
        def datesAreClose(date1: DateTime, date2: DateTime): Boolean = Math.abs(new Interval(date1, date2).toDurationMillis) <= 5000
        // Something is different, try to figure it out
        val valuesResult = if (remoteValue.value == localValue.value) "same-value" else "different-values"
        val closeDatesSuffix = if (datesAreClose(remoteValue.date, localValue.date)) "-but-close-dates" else ""
        val dateResult = if (remoteValue.date.isAfter(localValue.date))
          s"remote-is-newer-than-local$closeDatesSuffix"
        else if (localValue.date.isAfter(remoteValue.date))
          s"local-is-newer-than-remote$closeDatesSuffix"
        else if (localValue.date.isEqual(localValue.date))
          "same-date"
        else if (localValue.date.withZone(DateTimeZone.UTC).isEqual(localValue.date.withZone(DateTimeZone.UTC)))
          "same-date-on-utc"
        else
          "impossible-dates"
        val remoteExpired = remoteValue.hasExpired(ttl, now)
        val localExpired = localValue.hasExpired(ttl, now)
        val finalResult = s"$valuesResult-$dateResult-remote-expired-${remoteExpired}-local-expired-${localExpired}"
        logger.warn(s"sanityLocalValueCheck, key $key: got different results for local $localValue and remote $remoteValue ($finalResult)")
        reporter.onSanityLocalValueCheckFailedResult(key, finalResult, elapsedTime(startTime))
        // return remote to keep everyone consistent
        Future.successful(remoteValue.value)
      case Success(None) =>
        val localExpired = localValue.hasExpired(ttl, now)
        val finalResult = s"missing-remote-local-expired-${localExpired}"
        logger.warn(s"sanityLocalValueCheck, key $key: got local $localValue but no remote ($finalResult)")
        reporter.onSanityLocalValueCheckFailedResult(key, finalResult, elapsedTime(startTime))
        // Try generate it to keep a behaviour equivalent to remote only
        tryGenerateAndSet(key, genValue, startTime).map(_.value)
      case Failure(e) =>
        reporter.onRemoteError(key, e, elapsedTime(startTime))
        logger.warn(s"sanityLocalValueCheck, key: $key  failed to get remote", e)
        Future.successful(localValue.value)
    }
  }

  // Overwrite remote value without lock, retrying on error
  private def remoteOverwrite(key: String, calculatedValue: TimestampedValue[V], remote: RemoteCacheRW[TimestampedValue[V]], nanoStartTime: Long, currentRetry: Int = 0)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[TimestampedValue[V]] = {
    remote.set(key, calculatedValue).asTry().flatMap {
      case Success(_) =>
        reporter.onSuccessfullyRemoteSetValue(key, elapsedTime(nanoStartTime))
        logger.info(s"remoteForceSet successfully overwritten key $key")
        Future.successful(calculatedValue)
      case Failure(e) =>
        reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
        logger.warn(s"remoteForceSet, key $key: got error setting the value, scheduling retry $currentRetry of $maxErrorsToRetryOnRemote", e)
        // Retry failure
        after(backoffOnError, scheduler) {
          remoteOverwrite(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry + 1)
        }
    }
  }


  // Note: this method may return a failed future, but it will never cache it
  // Our main purpose here is to avoid multiple local calls to generate new promises/futures in parallel,
  // so we use this Map keep everyone in sync
  // This is similar to how spray cache works
  private def tryGenerateAndSet(key: String, genValue: () => Future[V], nanoStartTime: Long)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[TimestampedValue[V]] = {
    val promise = Promise[TimestampedValue[V]]()
    val future = promise.future
    tempUpdate.putIfAbsent(key, future) match {
      case null =>
        logger.info(s"tryGenerateAndSet, key $key: got request for generating and none in progress found, calling canonicalValueGenerator")
        canonicalValueGenerator(key, genValue, nanoStartTime).onComplete {
          case Success(v) if !v.hasExpired(ttl, now) =>
            reporter.onGeneratedWithSuccess(key, elapsedTime(nanoStartTime))
            localCache.foreach(_.set(key, v))
            promise.trySuccess(v)
            tempUpdate.remove(key, future)
          case Success(v) =>
            // Have we generated/got an expired value!?
            reporter.onUnexpectedBehaviour(key, elapsedTime(nanoStartTime))
            logger.warn(s"tryGenerateAndSet, key $key: unexpectedly generated/got an expired value: $v")
            localCache.foreach(_.set(key, v))
            promise.trySuccess(v)
            tempUpdate.remove(key, future)
          case Failure(e) =>
            // We don't save failures to cache
            // There is no need to log here, canonicalValueGenerator will log everything already
            reporter.onGeneratedWithFailure(key, e, elapsedTime(nanoStartTime))
            promise.tryFailure(e)
            tempUpdate.remove(key, future)
        }
        future
      case fTrying =>
        // If someone call us while a future is running, we return the running future
        logger.info(s"tryGenerateAndSet, key $key: got request for generating but an existing one is current in progress")
        fTrying
    }
  }

  // This can be called by multiple instances/hosts simultaneously but in the end
  // only the one that wins the race will create the final value that will be set in
  // the remote cache and read by the other instances
  // Unless of course there is some error getting stuff from remote cache
  // in which case the locally generated value may be returned to avoid further delays
  protected def canonicalValueGenerator(key: String, genValue: () => Future[V], nanoStartTime: Long)(implicit ec: ExecutionContext, scheduler: Scheduler) = {
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
                                       currentRetry: Int = 0)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[TimestampedValue[V]] = {
    remote.get(key).asTry().flatMap {
      case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
        logger.info(s"remoteGetNonExpiredValue, key $key: got a good value")
        Future.successful(remoteValue)
      case Success(_) =>
        Future.failed(new Exception("No good value found on remote"))
      case Failure(e) =>
        if (currentRetry >= maxErrorsToRetryOnRemote) {
          reporter.onRemoteGiveUp(key, elapsedTime(nanoStartTime))
          logger.error(s"remoteGetNonExpiredValue, key $key: returning calculated value because we got more than $maxErrorsToRetryOnRemote errors", e)
          Future.failed(e)
        } else {
          reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
          logger.warn(s"remoteGetNonExpiredValue, key $key: got error trying to get value, scheduling retry $currentRetry of $maxErrorsToRetryOnRemote", e)
          // Retry
          after(backoffOnError, scheduler) {
            remoteGetNonExpiredValue(key, remote, nanoStartTime, currentRetry = currentRetry + 1)
          }
        }
    }
  }

  // This methods tries to guarantee that everyone that calls it in
  // a given moment will be left with the same value in the end
  private def remoteSetOrGet(key: String,
                             calculatedValue: TimestampedValue[V],
                             remote: RemoteCacheRW[TimestampedValue[V]],
                             nanoStartTime: Long,
                             currentRetry: Int = 0)(implicit ec: ExecutionContext, scheduler: Scheduler): Future[TimestampedValue[V]] = {
    if (currentRetry > maxErrorsToRetryOnRemote) {
      // Use our calculated value as it's the best we can do
      reporter.onRemoteGiveUp(key, elapsedTime(nanoStartTime))
      logger.error(s"remoteSetOrGet, key $key: returning calculated value because we got more than $maxErrorsToRetryOnRemote errors")
      Future.successful(calculatedValue)
    } else {
      remote.get(key).asTry().flatMap {
        case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
          // Current value is good, just return it
          reporter.onRemoteCacheHitAfterGenerating(key, elapsedTime(nanoStartTime))
          logger.info(s"remoteSetOrGet, $key found already a good value on remote")
          Future.successful(remoteValue)
        case Success(remoteValue) =>
          // The remote value is missing or has expired. This is what we were expecting
          // We will replace the value with a CAS operation
          remote.replace(key, remoteValue, calculatedValue).asTry().flatMap {
            case Success(true) =>
              // Flawless victory!
              reporter.onSuccessfullyRemoteSetValue(key, elapsedTime(nanoStartTime))
              logger.info(s"remoteSetOrGet successfully set key $key using CAS")
              Future.successful(calculatedValue)
            case Success(false) =>
              // CAS operation failed, perhaps we took too long to set?
              reporter.onStillTryingToLockOrGet(key, elapsedTime(nanoStartTime))
              logger.warn(s"remoteSetOrGet, key $key failed to replace value $remoteValue using CAS, retrying...")
              remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry)
            case Failure(e) =>
              reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
              logger.warn(s"remoteSetOrGet, key $key: got error setting the value, scheduling retry $currentRetry of $maxErrorsToRetryOnRemote", e)
              // Retry failure
              after(backoffOnError, scheduler) {
                remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry + 1)
              }
          }
        case Failure(e) =>
          reporter.onRemoteError(key, e, elapsedTime(nanoStartTime))
          logger.warn(s"remoteSetOrGet, key $key: got error getting remote value, scheduling retry $currentRetry of $maxErrorsToRetryOnRemote", e)
          // Retry failure
          after(backoffOnError, scheduler) {
            remoteSetOrGet(key, calculatedValue, remote, nanoStartTime, currentRetry = currentRetry + 1)
          }
      }
    }
  }


}