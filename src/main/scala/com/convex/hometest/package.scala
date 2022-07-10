package com.convex

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

package object hometest {

  def initSpark(appName: String): SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("com.convex").setLevel(Level.INFO)
    spark
  }

  implicit val spark: SparkSession = initSpark("ConvexHomeTest")
}
