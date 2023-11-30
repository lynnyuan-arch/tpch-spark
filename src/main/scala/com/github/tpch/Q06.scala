package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.sum

/**
 * TPC-H Query 6
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q06 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
   /** import schemaProvider._
    import sqlContext.implicits._

    lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))**/


    val sql = "select\n\tsum(l_extendedprice * l_discount) as revenue\nfrom\n\tlineitem\nwhere\n\tl_shipdate >= date '1994-01-01'\n\tand l_shipdate < date '1994-01-01' + interval '1' year\n\tand l_discount between 0.05 and 0.07\n\tand l_quantity < 24\nLIMIT 1"

    println(s"Q06:\n $sql")
    sqlContext.sql(sql)
  }

}
