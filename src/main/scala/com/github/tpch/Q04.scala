package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.count

/**
 * TPC-H Query 4
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q04 extends TpchQuery {

  override def execute(sqlContext: SQLContext, schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
   /** import schemaProvider._
    import sqlContext.implicits._

    val forders = orders.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey")
      .distinct

    flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")
    **/

    val sql = "select\n\to_orderpriority,\n\tcount(*) as order_count\nfrom\n\torders\nwhere\n\to_orderdate >= date '1993-07-01'\n\tand o_orderdate < date '1993-10-01' + interval '3' month\n\tand exists (\n\t\tselect\n\t\t\t*\n\t\tfrom\n\t\t\tlineitem\n\t\twhere\n\t\t\tl_orderkey = o_orderkey\n\t\t\tand l_commitdate < l_receiptdate\n\t)\ngroup by\n\to_orderpriority\norder by\n\to_orderpriority\nLIMIT 1"
    println(s"Q04:\n $sql")
    sqlContext.sql(sql)

  }

}
