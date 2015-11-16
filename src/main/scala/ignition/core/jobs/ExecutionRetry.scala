package ignition.core.jobs

import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait ExecutionRetry {

  protected def executeRetrying[T](code: => T, maxExecutions: Int, delay: FiniteDuration): Try[T] = {
    assert(maxExecutions > 0)
    try {
      Success(code)
    } catch {
      case NonFatal(e) if maxExecutions > 1 =>
        // TODO: Log execution retries
        Thread.sleep(delay.toMillis)
        executeRetrying(code, maxExecutions - 1, delay * 2)
      case e: Throwable => Failure(e)
    }
  }

  protected def executeRetrying[T](code: => T, maxExecutions: Int = 3): T = {
    // TODO: log retries
    executeRetrying(code, maxExecutions - 1, 0.second).get
  }
}
