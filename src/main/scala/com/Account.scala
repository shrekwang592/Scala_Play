package com

import java.util.Date

import com.common.conf.SparkConfiguration
import org.apache.spark.sql.functions.{current_date, date_sub, first, sum}

case class Account(siteId: Int, sale: Double, InventoryValue: Int, incomingInventory: Int, date: Date)

object Account extends SparkConfiguration{
  val df = spark.read.json("./src/main/resources/account.json")
  df.createOrReplaceTempView("account")
  val currentDF = spark.sql("SELECT * from account where date =" + current_date)
  val preDF = spark.sql("SELECT * from account where date = " + date_sub(current_date, 1))
  def getAccountView ={
    val sqlDF = spark.sql("SELECT * from account")
    sqlDF.show()
  }

  def getCurrentDateGrossSale: Double = {
    val result = currentDF.agg(sum("sales")).first.get(0).toString
    result.toDouble
  }

  def getCurrentInventoryValue: Int ={
    val result = currentDF.agg(sum("inventory")).first.get(0).toString
    result.toInt
  }

  def getInventoryChange(siteId:Int): Int ={
    val result = spark.sql("SELECT inventory from account where date = " + date_sub(current_date, 1) + " and siteId = " + siteId)
    val newResult = spark.sql("SELECT inventory from account where date = " + current_date + " and siteId = " + siteId)
    newResult.agg(sum("inventory")).first.get(0).toString.toInt - result.agg(sum("inventory")).first.get(0).toString.toInt
  }

  def getCurrentNetSales:Double ={
    val sales = currentDF.agg(sum("sales")).first.get(0).toString.toDouble
    val incomingInv = currentDF.agg(sum("incomingInv")).first.get(0).toString.toDouble
    sales - incomingInv
  }
}

