package com.common.conf

import org.apache.spark.sql.SparkSession

class SparkConfiguration() {

  val spark: SparkSession = SparkSession.builder().
    config("spark.sql.warehouse.dir", "file:///tmp/spark-warehousepath")
    .appName("Accounting")
    .config("spark.master", "local")
    .getOrCreate()
}