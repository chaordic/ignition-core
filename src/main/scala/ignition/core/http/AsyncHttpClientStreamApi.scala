package ignition.core.http

import java.io.InputStream
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import ignition.core.utils.URLUtils
import spray.http._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object AsyncHttpClientStreamApi {
  
  case class Credentials(user: String, password: String) {
    def isEmpty = user.isEmpty && password.isEmpty

    def toOption = Some(this).filter(!_.isEmpty)
  }

  object Credentials {
    val empty = Credentials("", "")
  }

  // TODO: return a stream is dangerous because implies into a lock
  case class StreamResponse(status: Int, content: InputStream)

  // If any value is None, it will fallback to the implementation's default
  object RequestConfiguration {
    val defaultMaxRedirects: Int = 15
    val defaultMaxConnectionsPerHost: Int = 500
    val defaultPipelining: Boolean = false
    val defaultIdleTimeout: FiniteDuration = Duration(30, TimeUnit.SECONDS)
    val defaultRequestTimeout: FiniteDuration = Duration(20, TimeUnit.SECONDS)
    val defaultConnectingTimeout: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  }

  case class RequestConfiguration(maxRedirects: Option[Int] = Option(RequestConfiguration.defaultMaxRedirects),
                                  maxConnectionsPerHost: Option[Int] = Option(RequestConfiguration.defaultMaxConnectionsPerHost),
                                  pipelining: Option[Boolean] = Option(RequestConfiguration.defaultPipelining),
                                  idleTimeout: Option[Duration] = Option(RequestConfiguration.defaultIdleTimeout),
                                  requestTimeout: Option[Duration] = Option(RequestConfiguration.defaultRequestTimeout),
                                  connectingTimeout: Option[Duration] = Option(RequestConfiguration.defaultConnectingTimeout))

  case class Request(url: String,
                     params: Map[String, String] = Map.empty,
                     credentials: Option[Credentials] = None,
                     method: HttpMethod = HttpMethods.GET,
                     body: HttpEntity = HttpEntity.Empty,
                     headers: List[HttpHeader] = List.empty,
                     requestConfiguration: Option[RequestConfiguration] = None) {

    def uri: Uri = {
      if (params.nonEmpty)
        URLUtils.parseUri(url).map(_.withQuery(params)).get
      else
        URLUtils.parseUri(url).get
    }
  }

  case class RequestException(message: String, response: StreamResponse) extends RuntimeException(message)

  object NoOpReporter extends ReporterCallback {
    def onRequest(request: Request): Unit = {}
    def onResponse(request: Request, status: Int): Unit = {}
    def onFailure(request: Request, status: Int): Unit = {}
    def onRetry(request: Request): Unit = {}
    def onGiveUp(request: Request): Unit = {}
    def onError(request: Request, error: Any): Unit = {}
  }

  abstract class ReporterCallback {
    def onRequest(request: Request): Unit
    def onResponse(request: Request, status: Int): Unit
    def onFailure(request: Request, status: Int): Unit
    def onRetry(request: Request): Unit
    def onGiveUp(request: Request): Unit
    def onError(request: Request, error: Any): Unit
  }
}

trait AsyncHttpClientStreamApi {

  def makeRequest(request: AsyncHttpClientStreamApi.Request, retryConf: RetryConf = RetryConf(), retryOnHttpStatus: Seq[Int] = List.empty)
                   (implicit timeout: Timeout, reporter: AsyncHttpClientStreamApi.ReporterCallback = AsyncHttpClientStreamApi.NoOpReporter): Future[AsyncHttpClientStreamApi.StreamResponse]

}