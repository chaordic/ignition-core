package ignition.core.jobs.utils

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, ShouldMatchers}


class PathUtilsSpec extends FlatSpec  with ShouldMatchers {

  "PathUtils" should "parse extract date from paths" in {
    PathUtils.extractDate("s3n:/mail-ignition/emails/2014_08_29T15_07_33UTC") shouldBe new DateTime(2014, 8, 29, 15, 7, 33)
    PathUtils.extractDate("s3n://mail-engine/output/20140708-153457/xxx") shouldBe new DateTime(2014, 7, 8, 15, 34, 57)
    PathUtils.extractDate("s3n://event-processors/abandoned_cart/20140708_153457") shouldBe new DateTime(2014, 7, 8, 15, 34, 57)
    PathUtils.extractDate("s3n://platform-dumps/users/2014-06-05") shouldBe new DateTime(2014, 6, 5, 0, 0, 0)
    PathUtils.extractDate("s3n://chaordic-incremental-dumps/views/dt=2014-06-05/yyy") shouldBe new DateTime(2014, 6, 5, 0, 0, 0)
  }

  it should "fail when no date is found" in {
    intercept[Exception] {
      PathUtils.extractDate("s3n://chaordic-incremental-dumps/views/")
    }
  }

  it should "fail when multiple dates are found" in {
    intercept[Exception] {
      PathUtils.extractDate("s3n://chaordic-incremental-dumps/views/dt=2014-06-05/2014-06-05")
    }
  }

}
