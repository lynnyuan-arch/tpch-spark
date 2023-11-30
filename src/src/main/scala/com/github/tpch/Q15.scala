package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{max, sum, udf}

/**
 * TPC-H Query 15
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q15 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.

    /*    import schemaProvider._
        import sqlContext.implicits._

        val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

        val revenue = lineitem.filter($"l_shipdate" >= "1996-01-01" &&
          $"l_shipdate" < "1996-04-01")
          .select($"l_suppkey", decrease($"l_extendedprice", $"l_discount").as("value"))
          .groupBy($"l_suppkey")
          .agg(sum($"value").as("total"))
        // .cache

        revenue.agg(max($"total").as("max_total"))
          .join(revenue, $"max_total" === revenue("total"))
          .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
          .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
          .sort($"s_suppkey")*/


    val sql1 = "create temporary view revenue0 (supplier_no, total_revenue) as select l_suppkey,sum(l_extendedprice * (1 - l_discount)) from lineitem where l_shipdate >= date '1994-07-01' and l_shipdate < date '1994-07-01' + interval '3' month group by l_suppkey"
    println(s"Q15-01:\n $sql1")
    sqlContext.sql(sql1)

    val sql2 = "select\n\ts_suppkey,\n\ts_name,\n\ts_address,\n\ts_phone,\n\ttotal_revenue\nfrom\n\tsupplier,\n\trevenue0\nwhere\n\ts_suppkey = supplier_no\n\tand total_revenue = (\n\t\tselect\n\t\t\tmax(total_revenue)\n\t\tfrom\n\t\t\trevenue0\n\t)\norder by\n\ts_suppkey\nLIMIT 1"
    println(s"Q15-02:\n $sql2")
    val df = sqlContext.sql(sql2)
    val sql3 = "drop view revenue0"
    println(s"Q15-03:\n $sql3")
    sqlContext.sql(sql3)
    df
  }

}
