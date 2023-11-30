package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{avg, sum, udf}

/**
 * TPC-H Query 17
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q17 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
  /*
    import schemaProvider._
    import sqlContext.implicits._

    val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)
    */

    val sql = "select\n\tsum(l_extendedprice) / 7.0 as avg_yearly\nfrom\n\tlineitem,\n\tpart\nwhere\n\tp_partkey = l_partkey\n\tand p_brand = 'Brand#43'\n\tand p_container = 'JUMBO CAN'\n\tand l_quantity < (\n\tSELECT\n\t0.2 * avg(l_quantity)\n\tFROM\n\tlineitem\n\tWHERE\n\tp_partkey = l_partkey\n\t) "

    println(s"Q17:\n $sql")
    sqlContext.sql(sql)
  }

}
