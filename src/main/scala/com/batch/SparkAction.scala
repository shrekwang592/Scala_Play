package com.batch

import com.Account
import com.common.conf.SparkConfiguration

object SparkAction  extends  SparkConfiguration {

  def main(args: Array[String]): Unit = {
    Account.getAccountView
    println("Gross sale ===> " + Account.getCurrentDateGrossSale)
    println("Inventory value ===> " + Account.getCurrentInventoryValue)
    println("Inventory change for site 101 ===> " + Account.getInventoryChange(101))
    println("Net sales ===> " + Account.getCurrentNetSales)
    try {
      val typeOfLoad = args(0)
      val inputAdlsFolder = args(1)
      val outputAdlsFolder = args(2)
      val typeOfInputFile = args(3)
      val typeOfOutputFile = args(4)
      val primaryKey = args(5)

      if (typeOfLoad == "full") {
        new BatchFullLoad(inputAdlsFolder, outputAdlsFolder, typeOfInputFile, typeOfOutputFile)
      }
      else if (typeOfLoad == "increment") {
        new IncrLoad(inputAdlsFolder, outputAdlsFolder, typeOfInputFile, typeOfOutputFile, primaryKey)
      }
      else {
        print("Invalid Argument provided ")
      }
    }
    catch {
      case ae: ArrayIndexOutOfBoundsException =>
        println("Exception Message: " + ae.getMessage)
      case unknown: Exception =>
        println("Exception Message: " + unknown.getMessage)
    }
    finally {
      spark.stop()
    }
  }
}