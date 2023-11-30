package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 9
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q09 extends TpchQuery {

  override def execute(sqlContext: SQLContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(orders, $"l_orderkey" === orders("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      .sort($"n_name", $"o_year".desc)**/


    val sql = "select\n\tnation,\n\to_year,\n\tsum(amount) as sum_profit\nfrom\n\t(\n\t\tselect\n\t\t\tn_name as nation,\n\t\t\tyear(o_orderdate) as o_year,\n\t\t\tl_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n\t\tfrom\n\t\t\tpart,\n\t\t\tsupplier,\n\t\t\tlineitem,\n\t\t\tpartsupp,\n\t\t\torders,\n\t\t\tnation\n\t\twhere\n\t\t\ts_suppkey = l_suppkey\n\t\t\tand ps_suppkey = l_suppkey\n\t\t\tand ps_partkey = l_partkey\n\t\t\tand p_partkey = l_partkey\n\t\t\tand o_orderkey = l_orderkey\n\t\t\tand s_nationkey = n_nationkey\n\t\t\tand p_name like '%indian%'\n\t) as profit\ngroup by\n\tnation,\n\to_year\norder by\n\tnation,\n\to_year desc\nLIMIT 1"
    println(s"Q09:\n $sql")
    sqlContext.sql(sql)
  }

}
