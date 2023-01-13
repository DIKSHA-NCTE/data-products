package org.sunbird.diksha

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger
import org.sunbird.diksha.model.ETBMetricsModel

object ETBMetricsJob extends optional.Application with IJob {
  implicit val className: String = "org.sunbird.diksha.ETBMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    implicit val sparkContext: SparkContext = sc.getOrElse(null)
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, ETBMetricsModel)
    JobLogger.log("Job Completed.")
  }
}
