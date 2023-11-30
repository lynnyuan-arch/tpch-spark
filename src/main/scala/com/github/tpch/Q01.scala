package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{avg, count, sum, udf}

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q01 extends TpchQuery {

  override def execute(sqlContext: SQLContext, schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
    /*
    import sqlContext.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(decrease($"l_extendedprice", $"l_discount")),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")
    */
    val sql = "select\n\tl_returnflag,\n\tl_linestatus,\n\tsum(l_quantity) as sum_qty,\n\tsum(l_extendedprice) as sum_base_price,\n\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n\tsum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n\tavg(l_quantity) as avg_qty,\n\tavg(l_extendedprice) as avg_price,\n\tavg(l_discount) as avg_disc,\n\tcount(*) as count_order\nfrom\n\tlineitem\nwhere\n\tl_shipdate <= date '1998-09-02'\ngroup by\n\tl_returnflag,\n\tl_linestatus\norder by\n\tl_returnflag,\n\tl_linestatus \n\tLIMIT 1"
    println(s"Q01:\n $sql")
    sqlContext.sql(sql)
  }
}
