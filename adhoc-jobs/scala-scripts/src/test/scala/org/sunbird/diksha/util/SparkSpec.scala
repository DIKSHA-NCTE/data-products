package org.sunbird.diksha.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.{Event, JobContext}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.scalatest.BeforeAndAfterAll

/**
 * @author Santhosh
 */
class SparkSpec(val file: String = "src/test/resources/sample_telemetry.log") extends BaseSpec with BeforeAndAfterAll {

    var events: RDD[Event] = null
    implicit var sc: SparkContext = null

    override def beforeAll(): Unit = {
        super.beforeAll()
        JobLogger.init("org.ekstep.analytics.test-cases")
        sc = getSparkContext()
        events = loadFile[Event](file)
    }

    override def afterAll(): Unit = {
        super.afterAll()
        JobContext.cleanUpRDDs()
        CommonUtil.closeSparkContext()
    }

    def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
        if (file == null) {
            return null
        }
        val isString = mf.runtimeClass.getName.equals("java.lang.String")
        sc.textFile(file, 1).filter { x => !x.isEmpty() }.map { line =>
            {
                try {
                   if(isString) line.asInstanceOf[T] else JSONUtils.deserialize[T](line)
                } catch {
                    case ex: Exception =>
                        Console.err.println("Unable to parse line", line)
                        null.asInstanceOf[T]
                }
            }
        }.filter { x => x != null }.cache()
    }
}