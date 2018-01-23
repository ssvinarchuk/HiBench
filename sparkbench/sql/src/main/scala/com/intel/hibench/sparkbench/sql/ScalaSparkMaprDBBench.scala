package com.intel.hibench.sparkbench.sql

import com.mapr.db.MapRDB
import com.mapr.db.spark._
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object ScalaSparkMaprDBBench {
  private val PATH_TO_DBS = "/"

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        s"Usage: $ScalaSparkSQLBench <workload name> <SQL sciprt file>"
      )
      System.exit(1)
    }
    val workloadName = args(0)
    val resultTableName = args(1)
    val expectedTableName = args(2)
    val sqlFile = args(3)
    val sparkConf = new SparkConf().setAppName(workloadName)
    val spark = SparkSession.builder().appName(workloadName).config(sparkConf).enableHiveSupport().getOrCreate()
    val exampleSQLQuery = s"SELECT * FROM $expectedTableName"

    // generate universally unique identifier with help of UDF for _id field in dataFrame
    def uuid = java.util.UUID.randomUUID.toString
    val generateUUID = udf(() => uuid)

    val sqlCommand = scala.io.Source.fromFile(sqlFile).mkString
    sqlCommand.split(';').filter(_.trim.nonEmpty).foreach { spark.sql }

    println(s"fetch data from hive table $expectedTableName and add it into MapRDB")
    val data = spark.sql(exampleSQLQuery).withColumn("_id", generateUUID())

    if (MapRDB.tableExists(PATH_TO_DBS + expectedTableName)) {
      MapRDB.deleteTable(PATH_TO_DBS + expectedTableName)
    }
    data.saveToMapRDB(PATH_TO_DBS + expectedTableName, createTable = true)

    workloadName match {
      case "ScalaAggregation" => aggregationBench(spark, resultTableName, expectedTableName)
      case "ScalaJoin" => joinBench(spark, resultTableName, expectedTableName, args(4))
      case "ScalaScan" => scanBench(spark, resultTableName, expectedTableName)
      case "ScalaScanRDD" => scanRDDBench(spark.sparkContext, resultTableName, expectedTableName)
      case _ => println(s"$resultTableName function is not defined")
    }

    println(s"Data were saves in result table with name $resultTableName : " + MapRDB.tableExists(resultTableName))

    spark.stop()
  }

  private def aggregationBench(spark: SparkSession, resultTableName: String, expectedTableName: String): Unit = {
    println("Start aggregation bench")
    val dataFromDB = spark.loadFromMapRDB(PATH_TO_DBS + expectedTableName)

    // make actions on data for expected result
    val aggregatedData = dataFromDB.groupBy("sourceip").agg(sum("adrevenue"))
      .withColumn("_id", bin(monotonically_increasing_id()))

    if (MapRDB.tableExists(PATH_TO_DBS + resultTableName)) {
      MapRDB.deleteTable(PATH_TO_DBS + resultTableName)
    }

    aggregatedData.saveToMapRDB(PATH_TO_DBS + resultTableName, createTable = true)
  }

  private def joinBench(spark: SparkSession, resultTableName: String, expectedTableName: String, secondExpectedTableName: String): Unit = {
    println("Start join bench")
    // get all data from second hive table
    val queryForSecondTable = s"SELECT * FROM $secondExpectedTableName"
    // create _id field for data
    val data = spark.sql(queryForSecondTable).withColumn("_id", bin(monotonically_increasing_id()))

    // delete table if it exist before actions
    if (MapRDB.tableExists(PATH_TO_DBS + secondExpectedTableName)) {
      MapRDB.deleteTable(PATH_TO_DBS + secondExpectedTableName)
    }

    // save data from second table into MapRDB
    data.saveToMapRDB(PATH_TO_DBS + secondExpectedTableName, createTable = true)

    // get data from two tables
    val uservisitsData = spark.loadFromMapRDB(PATH_TO_DBS + expectedTableName)
    val rankingsData = spark.loadFromMapRDB(PATH_TO_DBS + secondExpectedTableName)

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

    if (MapRDB.tableExists(PATH_TO_DBS + resultTableName)) {
      MapRDB.deleteTable(PATH_TO_DBS + resultTableName)
    }

    result.saveToMapRDB(PATH_TO_DBS + resultTableName, createTable = true)
  }


  private def scanBench(spark: SparkSession, resultTableName: String, expectedTableName: String): Unit = {
    println("Start scan bench")
    val dataFromDB = spark.loadFromMapRDB(PATH_TO_DBS + expectedTableName)

    if (MapRDB.tableExists(PATH_TO_DBS + resultTableName)) {
      println(s"Delete table $resultTableName")
      MapRDB.deleteTable(PATH_TO_DBS + resultTableName)
    }

    dataFromDB.saveToMapRDB(PATH_TO_DBS + resultTableName, createTable = true)
  }

  private def scanRDDBench(sc: SparkContext, resultTableName: String, expectedTableName: String): Unit = {
    println("Start scan RDD bench")
    val rdd: RDD[OJAIDocument] = sc.loadFromMapRDB(PATH_TO_DBS + expectedTableName)

    if (MapRDB.tableExists(PATH_TO_DBS + resultTableName)) {
      println(s"Delete table $resultTableName")
      MapRDB.deleteTable(PATH_TO_DBS + resultTableName)
    }

    rdd.saveToMapRDB(PATH_TO_DBS + resultTableName, createTable = true)
  }
}
