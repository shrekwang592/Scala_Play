package com.batch

import java.util._

import com.common.conf.SparkConfiguration
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions._

class MetaDataLoad(srcPath: String, data: DataFrame, outputAdlsFolder: String, typeOfOutputFile: String)
  extends SparkConfiguration {
  try {
    val fileArrivalTime = current_timestamp()
    val filePathRdd = spark.sparkContext.wholeTextFiles(srcPath)
    val fileName = filePathRdd.keys.map( line => line.split("/").drop(8).mkString("")).collect()(0)

    val finalMetadataDf: DataFrame = data
      .withColumn("source_path", lit(srcPath))
      .withColumn("source_feedname", concat(lit(fileName), lit(fileArrivalTime)))
      .withColumn("file_arrival_datetime", lit(fileArrivalTime))
      .withColumn("id", lit(UUID.randomUUID().toString))
      .withColumn("record_process_datetime", lit(current_timestamp()))
    new BatchWrite(finalMetadataDf, outputAdlsFolder, typeOfOutputFile)
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