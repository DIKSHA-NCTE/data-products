package org.sunbird.diksha.report

import org.joda.time.DateTimeUtils
import org.scalatest.BeforeAndAfterAll
import org.sunbird.diksha.util.BaseSpec

class BaseReportSpec extends BaseSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    DateTimeUtils.setCurrentMillisFixed(1531047600000L)
    super.beforeAll()
    // Console.println("****** Starting embedded elasticsearch service ******")
    val spark = getSparkSession()
  }

  override def afterAll(): Unit = {
    //super.afterAll();
    // EmbeddedES.stop()
  }
  
}