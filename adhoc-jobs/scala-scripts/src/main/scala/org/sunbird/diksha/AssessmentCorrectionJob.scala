package org.sunbird.diksha

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.sunbird.diksha.model.AssessmentCorrectionModel

object AssessmentCorrectionJob extends IJob {

  implicit val className: String = "org.ekstep.analytics.job.AssessmentCorrectionJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    implicit val SparkContext: SparkContext = sc.orNull
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, AssessmentCorrectionModel)
    JobLogger.log("Job Completed.")
  }
}