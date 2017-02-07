package ignition.core.utils

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class URLUtilsSpec extends FlatSpec with Matchers {

  "URLUtils" should "add parameters to url with encoded params in base url and not be double encoded" in {
    val baseUrl: String = "https://tracker.client.com/product=1?email=user%40mail.com"
    val params = Map("cc" -> "second@mail.com")

    val result: String = URLUtils.addParametersToUrl(baseUrl, params)
    result shouldEqual "https://tracker.client.com/product=1?email=user%40mail.com&cc=second%40mail.com"
  }

  it should "add multiples params with the same name" in {
    val baseUrl: String = "https://tracker.client.com/product=1?email=user%40mail.com&cc=second%40mail.com"
    val params = Map("cc" -> "third@mail.com")

    val result: String = URLUtils.addParametersToUrl(baseUrl, params)
    result shouldEqual "https://tracker.client.com/product=1?email=user%40mail.com&cc=second%40mail.com&cc=third%40mail.com"
  }

  it should "works with Fragment in original URL" in {

    val baseUrl = "https://www.petlove.com.br/carrinho?utm_campanha=internalmkt#/add/variant_sku/310178,31012214/quantity/1?t=1"
    val params: Map[String, String] = Map(
      "utm_campaign" -> "abandonodecarrinho",
      "utm_source" -> "chaordic-mail",
      "utm_medium" -> "emailmkt",
      "cc" -> "second@mail.com"
    )

    val result = URLUtils.addParametersToUrl(baseUrl, params)

    val expected = "https://www.petlove.com.br/carrinho?utm_campanha=internalmkt&utm_campaign=abandonodecarrinho&utm_source=chaordic-mail&utm_medium=emailmkt&cc=second%40mail.com#/add/variant_sku/310178,31012214/quantity/1?t=1"

    result shouldEqual expected
  }

  it should "handle urls with new line character at the edges" in {
    val url = "\n\t\n\thttps://www.petlove.com.br/carrinho#/add/variant_sku/3105748-1,3107615/quantity/1?t=1\n\t"
    val finalUrl = URLUtils.addParametersToUrl(url, Map("test" -> "true"))
    finalUrl shouldEqual "https://www.petlove.com.br/carrinho?test=true#/add/variant_sku/3105748-1,3107615/quantity/1?t=1"
  }

  it should "percent encode url paths" in {
    val tests = Seq(
      "http://images1.petlove.com.br/products/170301/small/Ração-Special-Dog-Premium-Vegetais-Cenoura-e-Espinafre-para-Cães.jpg",
      "http://images0.petlove.com.br/products/175408/small/Ração-Nestlé-Purina-Pro-Plan-Cat-Sensitive-para-Gatos-Adultos-com-Pele-Sensível.jpg",
      "http://images3.petlove.com.br/products/171539/small/Ração-Royal-Canin-Feline-Veterinary-Diet-Urinary-SO-High-Dilution-para-Gatos-com-Cálculos-Urinários.jpg"
    )

    val expectations = Seq(
      "http://images1.petlove.com.br/products/170301/small/Ra%C3%A7%C3%A3o-Special-Dog-Premium-Vegetais-Cenoura-e-Espinafre-para-C%C3%A3es.jpg",
      "http://images0.petlove.com.br/products/175408/small/Ra%C3%A7%C3%A3o-Nestl%C3%A9-Purina-Pro-Plan-Cat-Sensitive-para-Gatos-Adultos-com-Pele-Sens%C3%ADvel.jpg",
      "http://images3.petlove.com.br/products/171539/small/Ra%C3%A7%C3%A3o-Royal-Canin-Feline-Veterinary-Diet-Urinary-SO-High-Dilution-para-Gatos-com-C%C3%A1lculos-Urin%C3%A1rios.jpg"
    )

    tests.zip(expectations).foreach {
      case (url, expected) => URLUtils.parseUri(url).map(_.toString) shouldBe Success(expected)
    }
  }

  it should "not encode percent characters in url path" in {
    val url = "http://www.example.com/Pentagrama%C2%AE Acessórios em São Paulo/Qualquer%20Arquivo%20Encodado.pdf"
    val sane = URLUtils.parseUri(url).map(_.toString)
    sane shouldBe Success("http://www.example.com/Pentagrama%C2%AE%20Acess%C3%B3rios%20em%20S%C3%A3o%20Paulo/Qualquer%20Arquivo%20Encodado.pdf")
  }

  it should "encode space characters with percent in URL path" in {
    val url = "http://www.example.com/Pentagrama+Invertido.xml?q=blah+bleh"
    val sane = URLUtils.parseUri(url).map(_.toString)
    sane shouldBe Success("http://www.example.com/Pentagrama%20Invertido.xml?q=blah+bleh")
  }
}
