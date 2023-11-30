package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 5
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q05 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val forders = orders.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)**/

    val sql = "select\n\tn_name,\n\tsum(l_extendedprice * (1 - l_discount)) as revenue\nfrom\n\tcustomer,\n\torders,\n\tlineitem,\n\tsupplier,\n\tnation,\n\tregion\nwhere\n\tc_custkey = o_custkey\n\tand l_orderkey = o_orderkey\n\tand l_suppkey = s_suppkey\n\tand c_nationkey = s_nationkey\n\tand s_nationkey = n_nationkey\n\tand n_regionkey = r_regionkey\n\tand r_name = 'ASIA'\n\tand o_orderdate >= date '1994-01-01'\n\tand o_orderdate < date '1994-01-01' + interval '1' year\ngroup by\n\tn_name\norder by\n\trevenue desc\nLIMIT 1"

    println(s"Q05:\n $sql")
    sqlContext.sql(sql)
  }

}
