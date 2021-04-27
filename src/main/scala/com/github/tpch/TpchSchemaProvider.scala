package com.github.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataFrameProvider {

  def customer: DataFrame
  def lineitem: DataFrame
  def nation: DataFrame
  def region: DataFrame
  def order: DataFrame
  def part: DataFrame
  def partsupp: DataFrame
  def supplier: DataFrame

  def initDataFrames(): Unit
}

abstract class TpchSchemaProvider (spark: SparkSession,
  format: String, options: scala.collection.immutable.Map[String, String]) extends DataFrameProvider {

  val dfReader = spark.read.format(format).options(options)

  import scala.collection.mutable.Map
  val dataFramesMap:Map[String, DataFrame] = Map[String, DataFrame]()

  initDataFrames()
  dataFramesMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }

  // for implicits
  override def customer:DataFrame = dataFramesMap.get("customer").get
  override def lineitem:DataFrame = dataFramesMap.get("lineitem").get
  override def nation:DataFrame = dataFramesMap.get("nation").get
  override def region:DataFrame = dataFramesMap.get("region").get
  override def order:DataFrame = dataFramesMap.get("order").get
  override def part:DataFrame = dataFramesMap.get("part").get
  override def partsupp:DataFrame = dataFramesMap.get("partsupp").get
  override def supplier:DataFrame = dataFramesMap.get("supplier").get
}