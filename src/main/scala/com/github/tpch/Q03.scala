package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 3
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q03 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val fcust = customer.filter($"c_mktsegment" === "BUILDING")
    val forders = orders.filter($"o_orderdate" < "1995-03-15")
    val flineitems = lineitem.filter($"l_shipdate" > "1995-03-15")

    fcust.join(forders, $"c_custkey" === forders("o_custkey"))
      .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
      .select($"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"o_orderdate", $"o_shippriority")
      .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc, $"o_orderdate")
      .limit(10)**/

    val sql  = "select\n\tl_orderkey,\n\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n\to_orderdate,\n\to_shippriority\nfrom\n\tcustomer,\n\torders,\n\tlineitem\nwhere\n\tc_mktsegment = 'BUILDING'\n\tand c_custkey = o_custkey\n\tand l_orderkey = o_orderkey\n\tand o_orderdate < date '1995-03-15'\n\tand l_shipdate > date '1995-03-15'\ngroup by\n\tl_orderkey,\n\to_orderdate,\n\to_shippriority\norder by\n\trevenue desc,\n\to_orderdate\nLIMIT 10"
    println(s"Q03:\n $sql")
    sqlContext.sql(sql)

  }

}
