package org.sunbird.diksha.model

import akka.stream.scaladsl.Source
import com.ing.wbaa.druid.client.DruidClient
import com.ing.wbaa.druid.{DruidConfig, DruidQuery, DruidResult}
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.diksha.util._
import akka.actor.ActorSystem
import java.time.{ZoneOffset, ZonedDateTime}

class TestTextbookProgressModel extends SparkSpec(null) with MockFactory{

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql")
  }

  "TextbookProgressModel" should "execute the model without any error" in {
    val config = """{"search":{"type":"none"},"model":"org.sunbird.diksha.model.TextBookProgressModel","modelParams":{"reportConfig":{"id":"content_progress_metrics","metrics":[],"labels":{"board":"Board","medium":"Medium","gradeLevel":"Grade","subject":"Subject","resourceType":"Content Type","live":"Live","review":"Review","draft":"Draft","application_ecml":"Created on Diksha","vedio_youtube":"YouTube Content","video_mp4":"Uploaded Videos","application_pdf":"Text Content","application_html":"Uploaded Interactive Content","creator":"Created By","status":"Status"},"output":[{"type":"csv","dims":[]}],"mergeConf":{"frequency":"WEEK","basePath":"","rollup":0,"reportPath":"content_progress_metrics.csv"}},"filter":{"tenantId":"ORG_001","slugName":""},"store":"local","format":"csv","key":"druid-reports/","filePath":"druid-reports/","container":"dev-data-store","folderPrefix":["slug","reportName"],"sparkCassandraConnectionHost":"localhost","sparkElasticsearchConnectionHost":"localhost"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Textbook Progress Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams
    implicit val mockFc = mock[FrameworkContext]
    implicit val sqlContext = new SQLContext(sc)

    //mocking for DruidDataFetcher
    val json: String =
      """
        |{"name":"Untitled Content","count":1.0,"createdOn":"2018-05-20T08:18:57.269+0000","createdFor":"01246375399411712074","channel":"01246375399411712074","identifier":"do_31250758113090764815944","resourceType":"Learn","creator":"Rohitash Bharia","mimeType":"video/mp4","contentType":"Resource","date":"1901-01-01","status":"Unlisted","pkgVersion":"0"}
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(List(druidResponse))).anyNumberOfTimes()
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    TextbookProgressModel.execute(sc.emptyRDD, jobConfig)
  }

  "Textbook progress model" should "execute with druid" in {
    val config = """{"search":{"type":"none"},"model":"org.sunbird.diksha.model.TextBookProgressModel","modelParams":{"reportConfig":{"id":"content_progress_metrics","metrics":[],"labels":{"board":"Board","medium":"Medium","gradeLevel":"Grade","subject":"Subject","resourceType":"Content Type","live":"Live","review":"Review","draft":"Draft","application_ecml":"Created on Diksha","vedio_youtube":"YouTube Content","video_mp4":"Uploaded Videos","application_pdf":"Text Content","application_html":"Uploaded Interactive Content","creator":"Created By","status":"Status"},"output":[{"type":"csv","dims":[]}]},"filter":{"tenantId":"ORG_001","slugName":""},"store":"local","format":"csv","key":"druid-reports/","filePath":"src/test/resources/druid-reports/","container":"test-container","folderPrefix":["slug","reportName"],"sparkCassandraConnectionHost":"localhost","sparkElasticsearchConnectionHost":"localhost"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Textbook Progress Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    implicit val mockFc = mock[FrameworkContext]
    implicit val sqlContext = new SQLContext(sc)

    //mocking for DruidDataFetcher
    val json: String =
      """
        |{"name":"Untitled Content","count":1.0,"createdOn":"2018-05-20T08:18:57.269+0000","createdFor":"01246375399411712074","channel":"01246375399411712074","identifier":"do_31250758113090764815944","resourceType":"Learn","creator":"Rohitash Bharia","mimeType":"video/mp4","contentType":"Resource","date":"1901-01-01","status":"Draft","pkgVersion":"0"}
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(List(druidResponse))).anyNumberOfTimes()
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    TextbookProgressModel.execute(sc.emptyRDD, jobConfig)
  }

  it should "execute if the tenantId is given but slugName is not given with slug as Unknown" in {
    val config = """{"search":{"type":"none"},"model":"org.sunbird.diksha.model.TextBookProgressModel","modelParams":{"reportConfig":{"id":"content_progress_metrics","metrics":[],"labels":{"board":"Board","medium":"Medium","gradeLevel":"Grade","subject":"Subject","resourceType":"Content Type","live":"Live","review":"Review","draft":"Draft","application_ecml":"Created on Diksha","vedio_youtube":"YouTube Content","video_mp4":"Uploaded Videos","application_pdf":"Text Content","application_html":"Uploaded Interactive Content","creator":"Created By","status":"Status"},"output":[{"type":"csv","dims":[]}]},"filter":{"tenantId":"ORG_001","slugName":""},"store":"local","format":"csv","key":"druid-reports/","filePath":"src/test/resources/druid-reports/","container":"test-container","folderPrefix":["slug","reportName"],"sparkCassandraConnectionHost":"localhost","sparkElasticsearchConnectionHost":"localhost"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Textbook Progress Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    implicit val mockFc = mock[FrameworkContext]
    implicit val sqlContext = new SQLContext(sc)

    //mocking for DruidDataFetcher
    val json: String =
      """
        |{"name":"Untitled Content","count":1.0,"createdOn":"2018-05-20T08:18:57.269+0000","createdFor":"ORG_01","channel":"01246375399411712074","identifier":"do_31250758113090764815944","resourceType":"Learn","creator":"Rohitash Bharia","mimeType":"video/mp4","contentType":"Resource","date":"1901-01-01","status":"Live","pkgVersion":"0"}
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(List(druidResponse))).anyNumberOfTimes()
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
    //    val res = TextbookUtils.getContentDataList("0123653943740170242", mockRestUtil)
    TextbookProgressModel.execute(sc.emptyRDD, jobConfig)
  }

  it should "execute if the tenantId is given with slugName" in {
    val config = """{"search":{"type":"none"},"model":"org.sunbird.diksha.model.report.TextBookProgressModel","modelParams":{"reportConfig":{"id":"content_progress_metrics","metrics":[],"labels":{"board":"Board","medium":"Medium","gradeLevel":"Grade","subject":"Subject","resourceType":"Content Type","live":"Live","review":"Review","draft":"Draft","application_ecml":"Created on Diksha","vedio_youtube":"YouTube Content","video_mp4":"Uploaded Videos","application_pdf":"Text Content","application_html":"Uploaded Interactive Content","creator":"Created By","status":"Status"},"output":[{"type":"csv","dims":[]}]},"filter":{"tenantId":"ORG_001","slugName":"Org"},"store":"local","format":"csv","key":"druid-reports/","filePath":"src/test/resources/druid-reports/","container":"test-container","folderPrefix":["slug","reportName"],"sparkCassandraConnectionHost":"localhost","sparkElasticsearchConnectionHost":"localhost"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Textbook Progress Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    implicit val mockFc = mock[FrameworkContext]
    implicit val sqlContext = new SQLContext(sc)

    //mocking for DruidDataFetcher
    val json: String =
      """
        |{"name":"Untitled Content","count":1.0,"createdOn":"2018-05-20T08:18:57.269+0000","createdFor":"01246375399411712074","channel":"01246375399411712074","identifier":"do_31250758113090764815944","resourceType":"Learn","creator":"Rohitash Bharia","mimeType":"video/mp4","contentType":"Resource","date":"1901-01-01","status":"Review","pkgVersion":"0"}
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(List(druidResponse))).anyNumberOfTimes()
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
    TextbookProgressModel.execute(sc.emptyRDD, jobConfig)
  }
}
