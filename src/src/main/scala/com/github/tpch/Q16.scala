package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{countDistinct, udf}

/**
 * TPC-H Query 16
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q16 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
/*

    import schemaProvider._
    import sqlContext.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { (x: Int) => x.toString().matches("49|14|23|45|19|3|36|9") }

    val fparts = part.filter(($"p_brand" !== "Brand#45") && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
*/

    val sql = "select\n\tp_brand,\n\tp_type,\n\tp_size,\n\tcount(distinct ps_suppkey) as supplier_cnt\nfrom\n\tpartsupp,\n\tpart\nwhere\n\tp_partkey = ps_partkey\n\tand p_brand <> 'Brand#14'\n\tand p_type not like 'LARGE ANODIZED%'\n\tand p_size in (39, 7, 49, 25, 26, 33, 14, 22)\n\tand ps_suppkey not in (\n\t\tselect\n\t\t\ts_suppkey\n\t\tfrom\n\t\t\tsupplier\n\t\twhere\n\t\t\ts_comment like '%Customer%Complaints%'\n\t)\ngroup by\n\tp_brand,\n\tp_type,\n\tp_size\norder by\n\tsupplier_cnt desc,\n\tp_brand,\n\tp_type,\n\tp_size\nLIMIT 1"

    println(s"Q16:\n $sql")
    sqlContext.sql(sql)
  }

}
