package ignition.core.utils

import java.net.{URLDecoder, URLEncoder}

import org.apache.http.client.utils.URIBuilder

import scala.util.Try

object URLUtils {

  // Due to ancient standards, Java will encode space as + instead of using percent.
  //
  // See:
  // http://stackoverflow.com/questions/1634271/url-encoding-the-space-character-or-20
  // https://docs.oracle.com/javase/7/docs/api/java/net/URLEncoder.html#encode(java.lang.String,%20java.lang.String)
  def sanitizePathSegment(segment: String): Try[String] =
    Try { URLEncoder.encode(URLDecoder.decode(segment, "UTF-8"), "UTF-8").replace("+", "%20") }

  def addParametersToUrl(url: String, partnerParams: Map[String, String]): String = {
    val builder = new URIBuilder(url.trim)
    partnerParams.foreach { case (k, v) => builder.addParameter(k, v) }
    builder.build().toString
  }
}
