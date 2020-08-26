package com.batch

import java.util.Properties

import com.common.conf.{Init, ReadConfiguration, SparkConfiguration}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row}

class IncrLoad (inputFolder: String, outputFolder: String, typeOfInputFile: String,
                  typeOfOutputFile: String, primaryKey: String) extends SparkConfiguration {
try {
  new Init()

  val propPath: Properties = new ReadConfiguration().configurationProperties("/config/application.config")
  val masterContainer: String = propPath.getProperty("masterContainer")
  val standardContainer: String = propPath.getProperty("standardContainer")

  val standardLatestPartition: String = new LatestPartition().getLatestPartition(standardContainer +
    inputFolder)
  val masterLatestPartition: String = new LatestPartition().getLatestPartition(masterContainer +
    outputFolder)

  if (typeOfInputFile == "orc") {
    val masterData: DataFrame = spark.read.orc(masterLatestPartition)
    val standardData: DataFrame = spark.read.orc(standardLatestPartition)
    masterData.createOrReplaceTempView("masterData")
    standardData.createOrReplaceTempView("standardData")
    val splitPrimaryKey = primaryKey.split(",")
    var queryFinal = ""
    for (x <- splitPrimaryKey) {
      queryFinal += "standardData." + x + " = masterData." + x + " and "
    }
    queryFinal = queryFinal.slice(0, queryFinal.length - 4)
    val joinDf = spark.sql("SELECT masterData.* FROM masterData " +
      "LEFT ANTI JOIN standardData ON" + queryFinal)
    val finalMasterDF = standardData.union(joinDf)
    new MetaDataLoad(standardLatestPartition, finalMasterDF, outputFolder, typeOfOutputFile)
     }
   }
   catch {
       
       case ae: AnalysisException =>
       println("Exception Message: " + ae.getMessage)
       case unknown: Exception =>
       println("Exception Message: " + unknown.getMessage)
     }
}