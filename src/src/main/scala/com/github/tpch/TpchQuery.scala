package com.github.tpch

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.CommandUtils

import java.io.{BufferedWriter, File, FileWriter}
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sqlContext: SQLContext, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite")
        .format("com.databricks.spark.csv")
        .option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: Int): ListBuffer[(String, Float)] = {
    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    val OUTPUT_DIR: String = "file:///" + new File(".").getAbsolutePath() + "/dbgen/output"
//    val OUTPUT_DIR: String = ""

//    val OUTPUT_DIR: String = "hdfs://node9.cloud.com:9000/spark/tpch/output_100g"

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum
      toNum = queryNum
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"com.github.tpch.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]


      outputDF(query.execute(spark.sqlContext, schemaProvider), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      println(s"cost ${query.getName()} -> $elapsed")
      results += Tuple2(query.getName(), elapsed)
    }

    results
  }

  def analyzeTable(spark: SparkSession, tableName: String): Unit = {
    val analyze = s"ANALYZE TABLE $tableName COMPUTE STATISTICS"
    println(analyze)
    spark.sql(analyze)
    val describe = s"DESCRIBE EXTENDED $tableName"
    spark.sql(describe).show()
  }

  def analyzeColumns(spark: SparkSession, tableName: String, columns: Array[String]): Unit = {
    if (columns.length <= 0) {
      return
    }
    val column = columns.foldRight("")(_+","+_)
    val analyze = s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS ${column.substring(0, column.length-1)}"
    println(analyze)
    spark.sql(analyze)
//    val describe  = s"DESCRIBE EXTENDED $tableName ${columns(0)}"
//    spark.sql(describe).show()
  }

  def analyzeTables(spark: SparkSession): Unit = {
    val tables = Array[String](
      "nation",
        "region",
      "part",
      "supplier",
      "partsupp",
      "customer",
      "orders",
      "lineitem"
    )
    tables.foreach(table => analyzeTable(spark, table))
  }

  def setTableMeta(spark: SparkSession): Unit = {
    val tables = Map[String, (Long, Long)](
      "nation" -> (25l, 2984l),
      "region" -> (5l, 544l),
      "part" -> (200000l, 30279322l),
      "supplier" -> (10000l, 1706027l),
      "partsupp" -> (800000l, 138192056l),
      "customer" -> (150000l, 28857678l),
      "orders" -> (1500000l, 199547456l),
      "lineitem" -> (6001215l, 799898635l)
    )

    tables.foreach(table => {
      val tableIdentWithDB = TableIdentifier(table._1, Option("default"))
      val tableMeta = spark.sessionState.catalog.getTableMetadata(tableIdentWithDB)
      val newStats = CommandUtils.compareAndGetNewStats(tableMeta.stats, BigInt(table._2._2), Some(BigInt(table._2._1)))
      println(s"newStats=$newStats")

      if (newStats.isDefined) {
        // 更新SparkSession的catalog中表的统计信息
        spark.sessionState.catalog.alterTableStats(tableIdentWithDB, newStats)
      }

      val describe = s"DESCRIBE EXTENDED ${table._1}"
      spark.sql(describe).show()
    })
  }

  def analyzeTableColumns(spark: SparkSession): Unit = {
    val columns = Map[String, Array[String]](
      "nation" -> Array[String](),
    "region" -> Array[String](),
    "part" -> Array[String]("p_type", "p_container"),  // "p_type", "p_container", "p_brand" 150, 40, 25
    "supplier" -> Array[String](),
    "partsupp" -> Array[String]("ps_supplycost"),
    "customer" -> Array[String](),  // c_phone
    "orders" -> Array[String]("o_orderdate"), //"o_orderstatus", 3
    "lineitem" -> Array[String]("l_shipdate", "l_receiptdate", "l_commitdate")
     // "l_discount", "l_quantity", "l_returnflag", "l_shipmode", "l_shipinstruct"
      // 11, 50, 3, 7, 4
    )

    columns.foreach(entry => {
      analyzeColumns(spark, entry._1, entry._2)
    })
  }

  // tpch_1g 0 false
  def main(args: Array[String]): Unit = {
    var queryNum = 0
    var database = "tpch_1g"
    var analyze = true
    if (args.length > 0)
      database = args(0)
    if (args.length > 1)
      queryNum = args(1).toInt // 0 --> all

    if (args.length > 2) {
      analyze = args(2).toBoolean
    }

    val spark = SparkSession.builder()
      //       向量读必须使用V2方式注册数据源
      //      .config("spark.znbase.vectorized.read.enable", true)
      //      .config("spark.znbase.vectorized.read.batch.size", 1000000)

      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.spark.sql.ZnExtensions")
      .config("spark.sql.znbase.pushDownPredicate", true)
      .config("spark.sql.znbase.pushDownSort", true)
      .config("spark.sql.znbase.pushDownAggregation", true)
      .config("spark.znbase.partition.filter", true)
      .config("spark.znbase.reader.batch", 100000)
      .config("spark.znbase.row.reader", "unsafe")
      .config("spark.znbase.follower.read.enable", true)
      .config("spark.znbase.read.replica.type", "column")
      .config("spark.driver.memory", "4G")
      .config("spark.executor.memory", "4G")
      .config("spark.sql.catalogImplementation", "in-memory") //hive, in-memory
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "4G")
      .config("spark.sql.adaptive.enabled", true)           // 自适应
      .config("spark.sql.adaptive.skewJoin.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .config("spark.sql.cbo.enabled", true)
//      .config("spark.sql.cbo.planStats.enabled", true)  // logical plan will fetch row counts and column statistics
//      .config("spark.sql.cbo.joinReorder.enabled", true)    // Enables join reorder in CBO
//      .config("spark.sql.cbo.joinReorder.dp.threshold", 12)   // SQLConf 节点数
//      .config("spark.sql.cbo.joinReorder.card.weight", 0.7)   //

//      .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
//      .config("spark.sql.codegen.wholeStage", false)

      // 计划输出
//      .config("spark.sql.optimizer.planChangeLog.level", "debug")
//      .config("spark.sql.optimizer.planChangeLog.batches", "Aggregate")

      .config("spark.sql.broadcastTimeout", 600)
      .appName("TPCH-Query-"+(if (queryNum == 0) "All" else  args(1)))
      .getOrCreate()

    val tuple = initZbSparkEnv(database)

    val schemaProvider = new TpchSchemaProvider(spark, tuple._1, tuple._2)

    // 分析数据表，生成直方图
    val cbo = spark.sparkContext.getConf.getBoolean("spark.sql.cbo.enabled", false)
    val histogram = spark.sparkContext.getConf.getBoolean("spark.sql.statistics.histogram.enabled", false)
    if (analyze && cbo) analyzeTables(spark)
    if (analyze && histogram) analyzeTableColumns(spark)
    //    setTableMeta(spark)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(spark, schemaProvider, queryNum)
    /*
    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()

*/
    output.foreach {
      case (key, value) => print(f"${key}%s\t${value}%1.8f\n")
    }

    TimeUnit.SECONDS.sleep(30)
    spark.close()
  }

  def initZbSparkEnv(database: String):(String, Map[String, String]) = {
    val addr = "node2.cloud.com"  //10.202.112.105 localhost
    val port = "9001"

    val format = "com.inspur.znbase"
    val options = Map[String, String]("znbase.addr" -> addr,
      "znbase.port" -> port,
      "database" -> database,
      "znbase.user" -> "root",
      "znbase.password" -> "ZNbase@2020",
      "znbase.sslrootcert" -> "/data/znbase-certs/certs/ca.crt",
      "znbase.sslcert" -> "/data/znbase-certs/certs/client.inspur.crt",
      "znbase.sslkey" -> "/data/znbase-certs/certs/client.inspur.pkcs8.key",
      "znbase.ssl" -> "false"
    )
    (format, options)
  }
}
