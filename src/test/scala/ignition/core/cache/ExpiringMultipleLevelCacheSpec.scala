package ignition.core.cache

import java.io.FileNotFoundException
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import ignition.core.cache.ExpiringMultiLevelCache.TimestampedValue
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}
import spray.caching.ExpiringLruLocalCache

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ExpiringMultipleLevelCacheSpec extends FlatSpec with Matchers with ScalaFutures {
  case class Data(s: String)
  implicit val scheduler = ActorSystem().scheduler

  "ExpiringMultipleLevelCache" should "calculate a value on cache miss and return it" in {
    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](1.minute, Option(local))
    Await.result(cache("key", () => Future.successful(Data("success"))), 1.minute) shouldBe Data("success")
  }

  it should "calculate a value on cache miss and return a failed future of the calculation" in {
    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](1.minute, Option(local))

    class MyException(s: String) extends Exception(s)

    val eventualCache = cache("key", () => Future.failed(new MyException("some failure")))
    whenReady(eventualCache.failed) { failure =>
      failure shouldBe a [MyException]
    }
  }

  it should "calculate a value on cache miss after ttl" in {
    val cacheTtl = 3.seconds
    val myRequestCount = new AtomicInteger()

    def myRequest(): Future[Data] = {
      myRequestCount.incrementAndGet()
      Future.successful(Data("success"))
    }

    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](ttl = cacheTtl, localCache = Option(local))

    whenReady(cache("key", myRequest)) { result =>
      result shouldBe Data("success")
    }

    myRequestCount.get() shouldBe 1

    whenReady(cache("key", myRequest)) { result =>
      result shouldBe Data("success")
    }

    myRequestCount.get() shouldBe 1

    val f = Future {
      Thread.sleep(cacheTtl.toMillis + 10)
    }

    whenReady(f, timeout(cacheTtl + 20.milli)) { _ =>
      whenReady(cache("key", myRequest)) { result =>
        result shouldBe Data("success")
      }

      myRequestCount.get() shouldBe 2

      whenReady(cache("key", myRequest)) { result =>
        result shouldBe Data("success")
      }

      myRequestCount.get() shouldBe 2
    }
  }

  it should "calculate a value on cache miss just once, the second call should be from cache hit" in {
    var myFailedRequestCount: Int = 0

    class MyException(s: String) extends FileNotFoundException(s) // Some NonFatal Exception
    def myFailedRequest(): Future[Nothing] = {
      myFailedRequestCount = myFailedRequestCount + 1
      Future.failed(new MyException("some failure"))
    }

    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](ttl = 1.minute, localCache = Option(local), cacheErrors = true, ttlCachedErrors = 9.seconds)

    val eventualCache = cache("key", myFailedRequest)
    whenReady(eventualCache.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

    val eventualCache2 = cache("key", myFailedRequest)
    whenReady(eventualCache2.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

    val eventualCache3 = cache("key", myFailedRequest)
    whenReady(eventualCache3.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

    val eventualCache4 = cache("key", myFailedRequest)
    whenReady(eventualCache4.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

    val eventualCache5 = cache("key", myFailedRequest)
    whenReady(eventualCache5.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

  }

  it should "calculate a value on cache miss on every request" in {
    var myFailedRequestCount: Int = 0

    class MyException(s: String) extends FileNotFoundException(s) // Some NonFatal Exception
    def myFailedRequest(): Future[Nothing] = {
      myFailedRequestCount = myFailedRequestCount + 1
      Future.failed(new MyException("some failure"))
    }

    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](ttl = 1.minute, localCache = Option(local), cacheErrors = false)

    val eventualCache = cache("key", myFailedRequest)
    whenReady(eventualCache.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

    val eventualCache2 = cache("key", myFailedRequest)
    whenReady(eventualCache2.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 2
    }

    val eventualCache3 = cache("key", myFailedRequest)
    whenReady(eventualCache3.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 3
    }

    val eventualCache4 = cache("key", myFailedRequest)
    whenReady(eventualCache4.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 4
    }

    val eventualCache5 = cache("key", myFailedRequest)
    whenReady(eventualCache5.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 5
    }

  }

  it should "calculate a value on cache miss, then wait ttlCachedError to get a cache miss again" in {
    var myFailedRequestCount: Int = 0

    class MyException(s: String) extends FileNotFoundException(s) // Some NonFatal Exception
    def myFailedRequest(): Future[Nothing] = {
      myFailedRequestCount = myFailedRequestCount + 1
      Future.failed(new MyException("some failure"))
    }

    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](ttl = 1.minute, localCache = Option(local), cacheErrors = true, ttlCachedErrors = 4.seconds)

    val eventualCache = cache("key", myFailedRequest)
    whenReady(eventualCache.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

    val eventualCache2 = cache("key", myFailedRequest)
    whenReady(eventualCache2.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 1
    }

    Thread.sleep(5000)

    val eventualCache3 = cache("key", myFailedRequest)
    whenReady(eventualCache3.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 2
    }

    val eventualCache4 = cache("key", myFailedRequest)
    whenReady(eventualCache4.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 2
    }

    Thread.sleep(500)

    val eventualCache5 = cache("key", myFailedRequest)
    whenReady(eventualCache5.failed) { failure =>
      failure shouldBe a [MyException]
      myFailedRequestCount shouldBe 2
    }

  }

}
