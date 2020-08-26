package com.batch

import java.util.{Calendar, Properties}

import com.common.conf.{ReadConfiguration, SparkConfiguration}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}

class BatchWrite(data: DataFrame, outputFolder: String, typeOfOutputFile: String)
  extends SparkConfiguration {
  try {
    val ReadAppConfig: Properties = new ReadConfiguration().configurationProperties("/config/application.config")
    val containerName: String = ReadAppConfig.getProperty("containerName")
    val storageAccountName: String = ReadAppConfig.getProperty("storageAccountName")

    val dateTime: Calendar = Calendar.getInstance()
    val Year: Int = dateTime.get(Calendar.YEAR)
    val Month: Int = dateTime.get(Calendar.MONTH)
    val Day: Int = dateTime.get(Calendar.DATE)

    val aOutputFolder: String = "abfss://%s@%s.dfs.core.windows.net".format(containerName, storageAccountName)
    val FilePath: String = aOutputFolder + outputFolder.dropRight(3) + "/" + Year + "/" + Month + "/" + Day

    if(typeOfOutputFile == "orc") {
      data.show(false)
      print("FilePath" + FilePath)
      data.write.format("orc").mode(SaveMode.Append)
        .save(FilePath)
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
