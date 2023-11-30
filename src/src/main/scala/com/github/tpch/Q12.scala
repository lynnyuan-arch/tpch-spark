package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 12
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q12 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._

    val mul = udf { (x: Double, y: Double) => x * y }
    val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(orders, $"l_orderkey" === orders("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")).as("sum_highorderpriority"),
        sum(lowPriority($"o_orderpriority")).as("sum_loworderpriority"))
      .sort($"l_shipmode")**/


    val sql = "select\n\tl_shipmode,\n\tsum(case\n\t\twhen o_orderpriority = '1-URGENT'\n\t\t\tor o_orderpriority = '2-HIGH'\n\t\t\tthen 1\n\t\telse 0\n\tend) as high_line_count,\n\tsum(case\n\t\twhen o_orderpriority <> '1-URGENT'\n\t\t\tand o_orderpriority <> '2-HIGH'\n\t\t\tthen 1\n\t\telse 0\n\tend) as low_line_count\nfrom\n\torders,\n\tlineitem\nwhere\n\to_orderkey = l_orderkey\n\tand l_shipmode in ('REG AIR', 'AIR')\n\tand l_commitdate < l_receiptdate\n\tand l_shipdate < l_commitdate\n\tand l_receiptdate >= date '1996-01-01'\n\tand l_receiptdate < date '1996-01-01' + interval '1' year\ngroup by\n\tl_shipmode\norder by\n\tl_shipmode\nLIMIT 1"

    println(s"Q12:\n $sql")
    sqlContext.sql(sql)
  }

}
