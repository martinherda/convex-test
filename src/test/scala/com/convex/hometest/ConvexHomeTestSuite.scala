package com.convex.hometest

import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SaveMode}
import spark.implicits._
import org.apache.spark.rdd.RDD

class ConvexHomeTestSuite extends FunSuite {
  //mock input data
  val mockPath: String = MockData.mockInputData()
  val mockRDD: RDD[(Int, Int)] = spark.sparkContext.parallelize(Seq(
    (1, 2),
    (1, 3),
    (1, 3),
    (2, 4),
    (2, 4),
    (2, 4),
    (2, 5),
    (2, 5),
    (2, 5),
    (2, 5),
    (3, 0),
    (3, 0)))

  test("should load files to RDD") {
    val rddFromFile = ConvexHomeTest.loadToRDD(mockPath)
    val result: Array[String] = rddFromFile.collect()

    assert(result.length.equals(2))
    assert(result.sameElements(
      Array("1,2\n1,3\n1,3", "2\t4\n2\t4\n2\t4\n2\t5\n2\t5\n2\t5\n2\t5\n3\t\"\"\n3\t\"\"")))
  }

  test("should parse RDD") {
    val rddFromFile = ConvexHomeTest.loadToRDD(mockPath)
    val parsedRDD = ConvexHomeTest.parseRDD(rddFromFile)
    val result: Array[(Int, Int)] = parsedRDD.collect()

    assert(result.length.equals(12))
    assert(result.sameElements(
      Array((1, 2), (1, 3), (1, 3), (2, 4), (2, 4), (2, 4), (2, 5), (2, 5), (2, 5), (2, 5), (3, 0), (3, 0))))
    assert(result.sameElements(mockRDD.collect()))
  }

  test("should filter odd occurrences using reduceByKey") {
    val result = ConvexHomeTest.filterOddOccurrences(mockRDD).collect()

    assert(result.length.equals(2))
    assert(result.sameElements(Array((1, 2), (2, 4))))
  }

  test("should filter odd occurrences using groupByKey") {
    val result = ConvexHomeTest.filterOddOccurrences2(mockRDD).collect()

    assert(result.length.equals(2))
    assert(result.sameElements(Array((1, 2), (2, 4))))
  }

  test("should filter odd occurrences using countByValue") {
    val result = ConvexHomeTest.filterOddOccurrences3(mockRDD).collect()

    assert(result.length.equals(2))
    assert(result.sameElements(Array((1, 2), (2, 4))))
  }
}

case class MockData(df: DataFrame, targetPath: String, writeOptions: Map[String, String])

object MockData {

  def mockInputData(): String = {
    val mockPath = "input"

    val dfCSV = List(
      ("1", "2"),
      ("1", "3"),
      ("1", "3")
    ).toDF("key", "value")

    val dfTSV: DataFrame = List(
      ("2", "4"),
      ("2", "4"),
      ("2", "4"),
      ("2", "5"),
      ("2", "5"),
      ("2", "5"),
      ("2", "5"),
      ("3", ""),
      ("3", null)
    ).toDF("key", "value")

    val mockedData = List(
      MockData(dfCSV, s"$mockPath/file_01", Map("header" -> "true", "delimiter" -> ",")),
      MockData(dfTSV, s"$mockPath/file_02", Map("header" -> "true", "delimiter" -> "\t"))
    )

    mockedData.foreach(m => {
      m.df.repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .options(m.writeOptions)
        .format("csv")
        .save(m.targetPath)
    })

    mockPath
  }
}
