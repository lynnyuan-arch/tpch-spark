package com.github.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}

class TpchParquetSchemaProvider(spark: SparkSession, path: String,
                                format: String, options: scala.collection.immutable.Map[String, String])
  extends TpchSchemaProvider(spark, format, options) {

  override def initDataFrames(): Unit = {
    dataFramesMap += ( "customer" -> dfReader.load(path+"/customer.parquet"))
    dataFramesMap += ("lineitem" -> dfReader.load(path+"/lineitem.parquet"))
    dataFramesMap += ("nation" -> dfReader.load(path+"/nation.parquet"))
    dataFramesMap += ("region" -> dfReader.load(path+"/region.parquet"))
    dataFramesMap += ("order" -> dfReader.load(path+"/orders.parquet"))
    dataFramesMap += ("part" -> dfReader.load(path+"/part.parquet"))
    dataFramesMap += ("partsupp" -> dfReader.load(path+"/partsupp.parquet"))
    dataFramesMap += ("supplier" -> dfReader.load(path+"/supplier.parquet"))
  }

}
