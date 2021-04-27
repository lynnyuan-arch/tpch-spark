package com.github.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}

class TpchDBSchemaProvider(spark: SparkSession,
                         format: String, options: Map[String, String])
  extends TpchSchemaProvider(spark, format, options) {

  override def initDataFrames(): Unit = {
    dataFramesMap + "customer" -> dfReader.option("table", "customer").load()
    dataFramesMap + "lineitem" -> dfReader.option("table", "lineitem").load()
    dataFramesMap + "nation" -> dfReader.option("table", "nation").load()
    dataFramesMap + "region" -> dfReader.option("table", "region").load()
    dataFramesMap + "order" -> dfReader.option("table", "orders").load()
    dataFramesMap + "part" -> dfReader.option("table", "part").load()
    dataFramesMap + "partsupp" -> dfReader.option("table", "partsupp").load()
    dataFramesMap + "supplier" -> dfReader.option("table", "supplier").load()
  }
}