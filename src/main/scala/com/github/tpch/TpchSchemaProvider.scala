package com.github.tpch

import org.apache.spark.sql.SparkSession

class TpchSchemaProvider(spark: SparkSession, format: String, options: Map[String, String]) {
  val dfReader = spark.read.format(format).options(options)


  val dataFramesMap = Map (
    "customer" -> dfReader
      .option("table", "customer")
      .load(),

    "lineitem" -> dfReader
      .option("table", "lineitem")
      .load(),

    "nation" -> dfReader
      .option("table", "nation")
      .load(),

    "region" -> dfReader
      .option("table", "region")
      .load(),

    "order" -> dfReader
      .option("table", "orders")
      .load(),

    "part" -> dfReader
      .option("table", "part")
      .load(),

    "partsupp" -> dfReader
      .option("table", "partsupp")
      .load(),

    "supplier" -> dfReader
      .option("table", "supplier")
      .load()
  )

  // for implicits
  val customer = dataFramesMap.get("customer").get
  val lineitem = dataFramesMap.get("lineitem").get
  val nation = dataFramesMap.get("nation").get
  val region = dataFramesMap.get("region").get
  val order = dataFramesMap.get("order").get
  val part = dataFramesMap.get("part").get
  val partsupp = dataFramesMap.get("partsupp").get
  val supplier = dataFramesMap.get("supplier").get

  dataFramesMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}