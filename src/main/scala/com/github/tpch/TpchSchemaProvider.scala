package com.github.tpch

import org.apache.spark.sql.SparkSession

class TpchSchemaProviderV2(spark: SparkSession, format: String, options: Map[String, String]) {
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

    "orders" -> dfReader
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
  val orders = dataFramesMap.get("orders").get
  val part = dataFramesMap.get("part").get
  val partsupp = dataFramesMap.get("partsupp").get
  val supplier = dataFramesMap.get("supplier").get

  dataFramesMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}

class TpchSchemaProvider(spark: SparkSession, format: String, options: Map[String, String]) {
  val tables = Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
  tables.foreach(name => {
    val createTbl = s"CREATE TABLE $name  \n" +
      s"USING znbase \n" +
      s"OPTIONS (\n" +
      s"  znbase.addr 'localhost', \n" +
      s"  znbase.port '26257', \n" +
      s"  database 'tpch', \n" +
      s"  table '$name'\n" +
      ")"

    println(createTbl)

    spark.sql(createTbl)
   /* val createView = s"CREATE TEMPORARY VIEW $name  " +
      s"USING znbase OPTIONS (" +
      s"znbase.addr 'localhost', " +
      s"znbase.port '26257', " +
      s"database 'defaultdb', " +
      s"table '$name')"

    spark.sql(createView)*/


  })


}