package com.intel.hibench.sparkbench.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import com.mapr.db.MapRDB
import com.mapr.db.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.lit

object ScalaSparkMaprDBBench {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        s"Usage: $ScalaSparkSQLBench <workload name> <SQL sciprt file>"
      )
      System.exit(1)
    }
    val workload_name = args(0)
    val resultTableName = args(1)
    val expectedTableName = args(2)
    val sql_file = args(3)
    val sparkConf = new SparkConf().setAppName(workload_name)
    val spark = SparkSession.builder().appName(workload_name).config(sparkConf).enableHiveSupport().getOrCreate()
    val exampleSQLQuery = s"SELECT * FROM $expectedTableName"


    val _sql = scala.io.Source.fromFile(sql_file).mkString
    _sql.split(';').filter(_.trim.nonEmpty).foreach { spark.sql }

    println(s"fetch data from hive table $expectedTableName and add it into MapRDB")
    val data = spark.sql(exampleSQLQuery).withColumn("_id", bin(monotonically_increasing_id()))

    if (MapRDB.tableExists(expectedTableName))
      MapRDB.deleteTable(expectedTableName)
    data.saveToMapRDB(expectedTableName, createTable = true)

    workload_name match {
      case "ScalaAggregation" => aggregationBench(spark, resultTableName, expectedTableName)
      case "ScalaJoin" => joinBench(spark, resultTableName, expectedTableName, args(4))
      case "ScalaScan" => scanBench(spark, resultTableName, expectedTableName)
      case _ => println(s"$resultTableName function is not defined")
    }

    spark.stop()
  }

  def aggregationBench(spark: SparkSession, resultTableName: String, expectedTableName: String) = {
    println("Start aggregation bench")
    // fetch data from MapRDB
    val dataFromDB = spark.loadFromMapRDB(expectedTableName)

    // make actions on data for expected result
    val aggregatedData = dataFromDB.groupBy("sourceip").agg(sum("adrevenue"))
      .withColumn("_id", bin(monotonically_increasing_id()))

    // remove result table if it exist
    if (MapRDB.tableExists(resultTableName))
      MapRDB.deleteTable(resultTableName)

    // save data to MapRDB
    aggregatedData.saveToMapRDB(resultTableName, createTable = true)
  }

  def joinBench(spark: SparkSession, resultTableName: String, expectedTableName: String, secondExpectedTableName: String) = {
    println("Start join bench")
    // get all data from second hive table
    val queryForSecondTable = s"SELECT * FROM $secondExpectedTableName"
    // create _id field for data
    val data = spark.sql(queryForSecondTable).withColumn("_id", bin(monotonically_increasing_id()))

    // delete table if it exist before actions
    if (MapRDB.tableExists(secondExpectedTableName))
      MapRDB.deleteTable(secondExpectedTableName)

    // save data from second table into MapRDB
    data.saveToMapRDB(secondExpectedTableName, createTable = true)

    // get data from two tables
    val uservisitsData = spark.loadFromMapRDB(expectedTableName)
    val rankingsData = spark.loadFromMapRDB(secondExpectedTableName)

    // filter uservisits data by date
    val prejoinedUservisitsData = uservisitsData.select("sourceip", "desturl", "adrevenue")
      .filter(uservisitsData("visitdate").geq(lit("1999-01-01")))
      .filter(uservisitsData("visitdate").leq(lit("2000-01-01")))

    // join rankings table with prejoined uservisits data
    val joinedData = rankingsData
      .join(prejoinedUservisitsData, rankingsData("pageurl") === prejoinedUservisitsData("desturl"))

    // make actions on data for expected result
    val result = joinedData.select("sourceip", "pagerank", "adrevenue")
      .groupBy("sourceip").agg(avg("pagerank"), sum("adrevenue"))
      .withColumn("_id", bin(monotonically_increasing_id()))

    // remove result table if it exist
    if (MapRDB.tableExists(resultTableName))
      MapRDB.deleteTable(resultTableName)

    // save data to MapRDB
    result.saveToMapRDB(resultTableName, createTable = true)
  }


  def scanBench(spark: SparkSession, resultTableName: String, expectedTableName: String) = {
    println("Start scan bench")
    // fetch data from MapRDB
    val dataFromDB = spark.loadFromMapRDB(expectedTableName)

    // remove result table if it exist
    if (MapRDB.tableExists(resultTableName))
      MapRDB.deleteTable(resultTableName)

    // save data to MapRDB
    dataFromDB.saveToMapRDB(resultTableName, createTable = true)
  }
}
