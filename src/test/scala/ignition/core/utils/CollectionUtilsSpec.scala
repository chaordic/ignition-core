package ignition.core.utils

import org.scalatest._
import CollectionUtils._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

class CollectionUtilsSpec extends FlatSpec with Matchers {

  case class MyObj(property: String, value: String)
  "CollectionUtils" should "provide distinctBy" in {
    val list = List(MyObj("p1", "v1"), MyObj("p2", "v1"), MyObj("p1", "v2"), MyObj("p2", "v2"))
    list.distinctBy(_.property) shouldBe List(MyObj("p1", "v1"), MyObj("p2", "v1"))
    list.distinctBy(_.value) shouldBe List(MyObj("p1", "v1"), MyObj("p1", "v2"))
  }

  it should "provide compress" in {
    List("a", "a", "b", "c", "e", "e", "c", "d", "e").compress shouldBe List("a", "b", "c", "e", "c", "d", "e")
  }

  it should "provide compress that works on empty lists" in {
    val list = List.empty
    list.compress shouldBe list
  }

  it should "provide compress that works on lists with only one element" in {
    val list = List(MyObj("p1", "v1"))
    list.compress shouldBe list
  }

  it should "provide compressBy" in {
    val list = List(MyObj("p1", "v1"), MyObj("p2", "v1"), MyObj("p1", "v2"), MyObj("p2", "v2"))
    list.compressBy(_.property) shouldBe list
    list.compressBy(_.value) shouldBe List(MyObj("p1", "v1"), MyObj("p1", "v2"))
  }

  it should "provide orElseIfEmpty" in {
    Seq.empty[String].orElseIfEmpty(Seq("something")) shouldBe Seq("something")
    Seq("not empty").orElseIfEmpty(Seq("something")) shouldBe Seq("not empty")
  }

  it should "provide maxOption and minOption" in {
    Seq.empty[Int].maxOption shouldBe None
    Seq(1, 3, 2).maxOption shouldBe Some(3)

    Seq.empty[Int].minOption shouldBe None
    Seq(1, 3, 2).minOption shouldBe Some(1)
  }

  it should "provide mostFrequentOption" in {
    Seq.empty[String].mostFrequentOption shouldBe None
    Seq("a", "b", "b", "c", "a", "b").mostFrequentOption shouldBe Option("b")
  }

  ".firstSuccessfulAttempt" should "run futures sequentially and pick the first successful future" in {
    val r1 = List(1,2,3).firstSuccessfulAttempt { n => Future(n) }
    val r2 = List(1,2,3).firstSuccessfulAttempt { n => if(n == 1) Future(n) else Future.failed(new Exception(n.toString)) }
    val r3 = List(1,2,3).firstSuccessfulAttempt { n => if(n == 2) Future(n) else Future.failed(new Exception(n.toString)) }
    val r4 = List(1,2,3).firstSuccessfulAttempt { n => if(n == 3) Future(n) else Future.failed(new Exception(n.toString)) }
    val r5 = List(1,2,3).firstSuccessfulAttempt { n => if(n == 1 || n == 2) Future(n) else Future.failed(new Exception(n.toString)) }
    val r6 = List(1,2,3).firstSuccessfulAttempt { n => if(n == 1 || n == 3) Future(n) else Future.failed(new Exception(n.toString)) }
    val r7 = List(1,2,3).firstSuccessfulAttempt { n => if(n == 2 || n == 3) Future(n) else Future.failed(new Exception(n.toString)) }
    val r8 = List(1,2,3).firstSuccessfulAttempt { n => Future.failed(new Exception(n.toString)) }
    val r9 = List.empty[Int].firstSuccessfulAttempt { n => Future.failed(new Exception(n.toString)) }

    Await.result(r1, 1.second) shouldBe Some(1)
    Await.result(r2, 1.second) shouldBe Some(1)
    Await.result(r3, 1.second) shouldBe Some(2)
    Await.result(r4, 1.second) shouldBe Some(3)
    Await.result(r5, 1.second) shouldBe Some(1)
    Await.result(r6, 1.second) shouldBe Some(1)
    Await.result(r7, 1.second) shouldBe Some(2)
    Await.result(r8, 1.second) shouldBe None
    Await.result(r9, 1.second) shouldBe None
  }

  ".scanUntilSuccess" should "run the futures sequentially and scan until first successful future is evaluated" in {
    val r1 = List(1,2,3).scanUntilSuccess { n => Future(n) }
    val r2 = List(1,2,3).scanUntilSuccess { n => if(n == 1) Future(n) else Future.failed(new Exception(n.toString)) }
    val r3 = List(1,2,3).scanUntilSuccess { n => if(n == 2) Future(n) else Future.failed(new Exception(n.toString)) }
    val r4 = List(1,2,3).scanUntilSuccess { n => if(n == 3) Future(n) else Future.failed(new Exception(n.toString)) }
    val r5 = List(1,2,3).scanUntilSuccess { n => if(n == 1 || n == 2) Future(n) else Future.failed(new Exception(n.toString)) }
    val r6 = List(1,2,3).scanUntilSuccess { n => if(n == 1 || n == 3) Future(n) else Future.failed(new Exception(n.toString)) }
    val r7 = List(1,2,3).scanUntilSuccess { n => if(n == 2 || n == 3) Future(n) else Future.failed(new Exception(n.toString)) }
    val r8 = List(1,2,3).scanUntilSuccess { n => Future.failed(new Exception(n.toString)) }
    val r9 = List.empty[Int].scanUntilSuccess { n => Future.failed(new Exception(n.toString)) }

    Await.result(r1, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq(1)
    Await.result(r2, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq(1)
    Await.result(r3, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq("1", 2)
    Await.result(r4, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq("1", "2", 3)
    Await.result(r5, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq(1)
    Await.result(r6, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq(1)
    Await.result(r7, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq("1", 2)
    Await.result(r8, 1.second).map(n => n.getOrElse(n.failed.get.getMessage)) shouldBe Seq("1", "2", "3")
    Await.result(r9, 1.second) shouldBe Seq.empty
  }
}
