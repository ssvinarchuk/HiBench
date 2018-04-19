package com.intel.hibench.sparkbench.structuredstreaming.application
import java.util.concurrent.atomic.AtomicLong

import com.intel.hibench.common.streaming.metrics.KafkaReporter
import com.intel.hibench.sparkbench.structuredstreaming.util.{MFSUtil, SparkBenchConfig}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

class StructuredGroupByFT extends StructuredBenchBase {
  var testCase: String = ""
  val startTime: AtomicLong = new AtomicLong(0)
  val rowProcessed: AtomicLong = new AtomicLong(0)



  override def process(df: DataFrame, config: SparkBenchConfig): Unit = {
    val spark = SparkSession.builder.appName("structured " + config.benchName).getOrCreate()
    import spark.implicits._

    testCase = config.benchName

    val streamQuery = df.selectExpr("CAST(value as STRING)")
      .as[String]
      .map(_.split(","))
      .selectExpr("CAST(value[0] AS STRING) AS ID", "CAST(value[1] AS STRING) val")
      .groupBy("ID").count()
      .writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("groupByQuery")
      .start()

    startTime.set(System.currentTimeMillis())

    startObserver(streamQuery, config)

    streamQuery.awaitTermination(config.executionTime)
  }

  def startObserver(query: StreamingQuery, config: SparkBenchConfig): Unit ={
    new Thread(new Runnable {
      override def run(): Unit = {
        while(!(isTableExists(query) &&
          checkResult(query, config.keyCount, config.totalRecords))) {

          Thread.sleep(1000)
        }
        writeResult(System.currentTimeMillis() - startTime.get())
        println("Stopping streaming session")
        query.stop()
      }
    }).start()
  }

  def isTableExists(query: StreamingQuery): Boolean = {
    query.sparkSession.catalog.tableExists(query.name)
  }

  def checkResult(query: StreamingQuery, keyCount: Long, totalRecords: Long): Boolean = {
    var result: Boolean = false
    val ds = query
      .sparkSession
      .sql("SELECT count from " + query.name).collect();

    if (ds.length == keyCount) {
      result = checkCount(ds, totalRecords)
    }
    result
  }
  def checkCount(rows: Array[Row], procRowNum: Long): Boolean = {
    var result: Boolean = true
    rowProcessed.set(0)
    for(row <-  rows) {
      val count = row.getLong(0)
      rowProcessed.addAndGet(count)
    }
    rowProcessed.get() == procRowNum
  }

  def writeResult(processTime: Long) : Unit = {
    val resultString = s"TestCase ${testCase} was finish. Processed ${rowProcessed.get()} rows," +
      s" time taken: ${processTime} ms"
    val resultPath = new Path(s"/${testCase}/${testCase}.res")

    MFSUtil.deleteIfExists(resultPath)
    MFSUtil.write(resultString, resultPath)
  }

}
