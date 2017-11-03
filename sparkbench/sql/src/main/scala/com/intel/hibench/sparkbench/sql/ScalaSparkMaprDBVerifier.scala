package com.intel.hibench.sparkbench.sql

import com.mapr.db.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}

object ScalaSparkMaprDBVerifier {

  val PATH_TO_DBS = "/"

  val ORDERED_COLUMNS: Array[Column] = Array(new Column("adrevenue"), new Column("countrycode"), new Column("desturl"),
    new Column("duration"), new Column("languagecode"), new Column("searchword"), new Column("sourceip"),
    new Column("useragent"), new Column("visitdate"))

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
    _sql.split(';').filter(_.trim.nonEmpty).foreach {
      spark.sql
    }

    println(s"Fetching data from hive table $expectedTableName and compare it with MapR DB Tables data ignoring '_id' column ...")

    val originalData = spark.sql(exampleSQLQuery).select(ORDERED_COLUMNS: _*)
    val maprDBData = spark.loadFromMapRDB(PATH_TO_DBS + resultTableName).drop("_id").select(ORDERED_COLUMNS: _*)

    val originalCount = originalData.count()
    val maprDBDataCount = maprDBData.count()
    if(originalCount != maprDBDataCount) {
      throw new IllegalStateException(s"MapR DB records count '$maprDBDataCount' does not match original '$originalCount'")
    }

    val diff = originalData.except(maprDBData)
    if(diff.count() != 0) {
      throw new IllegalStateException("MapR DB data does not match original data")
    }

    spark.stop()
  }

}
