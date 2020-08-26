package com.batch

import java.util.Properties

import com.common.conf.{Init, ReadConfiguration, SparkConfiguration}
import org.apache.spark.sql.{AnalysisException, DataFrame}

class BatchFullLoad(inputFolder: String, outputFolder: String, typeOfInputFile: String,
                        typeOfOutputFile: String) extends SparkConfiguration {
  try {
    new Init()
    val readConfig: Properties = new ReadConfiguration().configurationProperties("/config/application.config")
    val standardContainer: String = readConfig.getProperty("standardContainer")
    val maxPartitionPath: String = new LatestPartition().getLatestPartition(standardContainer + inputFolder)

    if (typeOfInputFile == "orc") {
      val loadData: DataFrame = spark.read.orc(maxPartitionPath)
      new MetaDataLoad( maxPartitionPath, loadData, outputFolder, typeOfOutputFile)
    }
  }
  catch {
    case ae: AnalysisException =>
      println("Exception Message: " + ae.getMessage)
      spark.stop()
    case unknown: Exception =>
      println("Exception Message: " + unknown.getMessage)
      spark.stop()
  }
}
