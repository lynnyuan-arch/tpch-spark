package com.github.tpch

import org.apache.spark.sql._

import java.io.{BufferedWriter, File, FileWriter}
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
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: Int): ListBuffer[(String, Float)] = {
    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    val OUTPUT_DIR: String = "file:///" + new File(".").getAbsolutePath() + "/dbgen/output"

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"com.github.tpch.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]


      outputDF(query.execute(spark.sqlContext, schemaProvider), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += Tuple2(query.getName(), elapsed)

    }

    results
  }

  def znsparkSchemaProvider(spark: SparkSession): TpchSchemaProvider = {
    val addr = "localhost"
    val port = "26257"
    val database = "tpch"

    val format = "com.inspur.znspark"
    val options = Map[String, String]("host" -> addr,
    "port" -> port,
    "database" -> database,
    "znspark.vectorized.read.enable" -> "false")

    new TpchDBSchemaProvider(spark, format, options)
  }

  def jdbcSchemaProvider(spark: SparkSession): TpchSchemaProvider = {
    val addr = "localhost"
    val port = "26257"
    val database = "tpch"

    val format = "com.inspur.znspark"
    val options = Map[String, String]("host" -> addr,
      "port" -> port,
      "database" -> database,
      "znspark.vectorized.read.enable" -> "false")

    new TpchDBSchemaProvider(spark, format, options)
  }

  def parquetSchemaProvider(spark: SparkSession): TpchSchemaProvider = {
    val path = "/data/tmp/spark-warehouse/tpch"

    val format = "parquet"
    val options = Map[String, String](
      "spark.sql.parquet.enableVectorizedReader" -> "false")

    new TpchParquetSchemaProvider(spark, path, format, options)
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0;
    if (args.length > 0)
      queryNum = args(0).toInt

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TPCH-Query-"+(if (queryNum == 0) "All" else  args(0)))
      .getOrCreate()

    val schemaProvider = parquetSchemaProvider(spark)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(spark, schemaProvider, queryNum)

    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
