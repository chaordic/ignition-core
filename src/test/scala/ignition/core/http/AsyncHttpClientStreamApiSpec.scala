package ignition.core.http

import ignition.core.http.AsyncHttpClientStreamApi.Request
import org.scalatest.{FunSpec, Matchers}

import scala.util.Success

class AsyncHttpClientStreamApiSpec extends FunSpec with Matchers {

  it("should do the best to parse the provided uri") {
    val url = "http://www.example.com/Pentagrama%C2%AE Acessórios em São Paulo/Qualquer%20Arquivo%20Encodado.pdf"
    val request = Request(url)
    request.uri.toString shouldBe "http://www.example.com/Pentagrama%C2%AE%20Acess%C3%B3rios%20em%20S%C3%A3o%20Paulo/Qualquer%20Arquivo%20Encodado.pdf"
  }
}
