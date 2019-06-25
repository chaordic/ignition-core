package ignition.core.utils

import org.scalatest.{FlatSpec, Matchers}

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

}
