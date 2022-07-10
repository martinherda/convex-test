package com.convex.hometest

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import java.io.IOException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object ConvexHomeTest extends Logging {

  /**
   * Loads source files to RDD[String]
   */
  def loadToRDD(sourcePath: String): RDD[String] = {
    val path = Path.mergePaths(new Path(sourcePath), new Path("/*")).toString
    spark.sparkContext.wholeTextFiles(path)
      //skip header
      .mapValues(v => v.split("\n").drop(1).mkString("\n"))
      .values
  }

  def replaceEmptyString(fields: Array[String]): Int = {
    if (fields.length < 2 || fields(1).replace("\"", "").trim.isEmpty)
      0
    else fields(1).replace("\"", "").toInt
  }

  /**
   * Parses csv/tsv stored in RDD[String] to RDD[(Int, Int)]
   */
  def parseRDD(rdd: RDD[String]): RDD[(Int, Int)] = {
    rdd.flatMap(lines => lines.split("\n"))
      .map(line => line.split(",|\t"))
      .map(fields => (fields(0).replace("\"", "").toInt, replaceEmptyString(fields)))
  }

  /**
   * Solution #1
   * Filters odd occurrences using reduceByKey
   */
  def filterOddOccurrences(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    rdd.map(row => (row, 1))
      .reduceByKey(_ + _)
      .filter(_._2 % 2 != 0)
      .keys
  }

  /**
   * Solution #2
   * Filters odd occurrences using groupByKey
   */
  def filterOddOccurrences2(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    rdd.map(row => (row, 1))
      .groupByKey()
      .mapValues(s => s.sum)
      .filter(_._2 % 2 != 0)
      .keys
  }

  /**
   * Solution #3
   * Filters odd occurrences using countByValue
   *
   * @note returns the Map to driver program so it’s suitable only for small datasets
   */
  def filterOddOccurrences3(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    val oddOccurrencesMap = rdd.countByValue().filter(_._2 % 2 != 0)
    val bOddOccurrencesMap = spark.sparkContext.broadcast(oddOccurrencesMap)
    rdd.filter(t => bOddOccurrencesMap.value.contains(t)).distinct()
  }

  /**
   * Saves the result as TSV file
   */
  def saveResult(rdd: RDD[(Int, Int)], targetPath: String, tmpPath: String): Unit = {
    rdd.coalesce(1)
      .map({ case (key, value) => Array(key, value).mkString("\t") })
      .saveAsTextFile(tmpPath)

    FsUtils.delete(new Path(targetPath))
    FsUtils.moveAtomic(new Path(tmpPath), new Path(targetPath))
  }

  def buildTmpPath(path: String): String = {
    path.substring(0, path.lastIndexOf(System.getProperty("file.separator"))) +
      System.getProperty("file.separator") + "tmp"
  }

  def main(args: Array[String]): Unit = {
    //configuration
    val inputPath = args(0)
    val outputPath = args(1)
    val tmpPath = buildTmpPath(outputPath)
    logInfo(
      s"""
         |Configuration:
         |inputPath = $inputPath
         |outputPath = $outputPath
         |tmpPath = $tmpPath
         |""".stripMargin)

    //clean tmpPath
    FsUtils.delete(new Path(tmpPath))

    logInfo(s"Loading source files to RDD")
    val rdd = loadToRDD(inputPath)

    logInfo(s"Parsing RDD[String] to RDD[(Int, Int)]")
    val parsedRDD = parseRDD(rdd)

    logInfo(s"Filtering odd occurrences")
    val filteredRDD = filterOddOccurrences(parsedRDD)

    logInfo(s"Saving result")
    saveResult(filteredRDD, outputPath, tmpPath)
  }
}

object FsUtils {

  def getFileSystem(path: Path): FileSystem = {
    path.getFileSystem(getHadoopConfiguration)
  }

  def getHadoopConfiguration: Configuration = {
    spark.sparkContext.hadoopConfiguration
  }

  def moveAtomic(source: Path, target: Path): Unit = {
    val fs = getFileSystem(target)
    val success = fs.rename(source, target)
    if (!success) {
      throw new IOException(s"Couldn't move source: $source to target: $target")
    }
  }

  def delete(toDelete: Path): Unit = {
    val fs = getFileSystem(toDelete)
    fs.delete(toDelete, true)
  }
}
