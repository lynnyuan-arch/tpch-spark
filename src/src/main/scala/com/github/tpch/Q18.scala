package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.sum

/**
 * TPC-H Query 18
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q18 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
  /*
    import schemaProvider._
    import sqlContext.implicits._

    lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(orders, orders("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .limit(100)
    */

    val sql = "select\n\tc_name,\n\tc_custkey,\n\to_orderkey,\n\to_orderdate,\n\to_totalprice,\n\tsum(l_quantity)\nfrom\n\tcustomer,\n\torders,\n\tlineitem\nwhere\n\to_orderkey in (\n\t\tselect\n\t\t\tl_orderkey\n\t\tfrom\n\t\t\tlineitem\n\t\tgroup by\n\t\t\tl_orderkey having\n\t\t\t\tsum(l_quantity) > 314\n\t)\n\tand c_custkey = o_custkey\n\tand o_orderkey = l_orderkey\ngroup by\n\tc_name,\n\tc_custkey,\n\to_orderkey,\n\to_orderdate,\n\to_totalprice\norder by\n\to_totalprice desc,\n\to_orderdate\nLIMIT 100"

    println(s"Q18:\n $sql")
    sqlContext.sql(sql)
  }

}
