package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 20
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q20 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
/*
    import schemaProvider._
    import sqlContext.implicits._

    val forest = udf { (x: String) => x.startsWith("forest") }

    val flineitem = lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01")
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

    val fnation = nation.filter($"n_name" === "CANADA")
    val nat_supp = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    part.filter(forest($"p_name"))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
      .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey").distinct
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address")
      .sort($"s_name")
    */

    val sql = "select\n\ts_name,\n\ts_address\nfrom\n\tsupplier,\n\tnation\nwhere\n\ts_suppkey in (\n\t\tselect\n\t\t\tps_suppkey\n\t\tfrom\n\t\t\tpartsupp,\n\t\t\t(\n\t\t\t\tselect\n\t\t\t\t\tl_partkey agg_partkey,\n\t\t\t\t\tl_suppkey agg_suppkey,\n\t\t\t\t\t0.5 * sum(l_quantity) AS agg_quantity\n\t\t\t\tfrom\n\t\t\t\t\tlineitem\n\t\t\t\twhere\n\t\t\t\t\tl_shipdate >= date '1997-01-01'\n\t\t\t\t\tand l_shipdate < date '1997-01-01' + interval '1' year\n\t\t\t\tgroup by\n\t\t\t\t\tl_partkey,\n\t\t\t\t\tl_suppkey\n\t\t\t) agg_lineitem\n\t\twhere\n\t\t\tagg_partkey = ps_partkey\n\t\t\tand agg_suppkey = ps_suppkey\n\t\t\tand ps_partkey in (\n\t\t\t\tselect\n\t\t\t\t\tp_partkey\n\t\t\t\tfrom\n\t\t\t\t\tpart\n\t\t\t\twhere\n\t\t\t\t\tp_name like 'brown%'\n\t\t\t)\n\t\t\tand ps_availqty > agg_quantity\n\t)\n\tand s_nationkey = n_nationkey\n\tand n_name = 'IRAQ'\norder by\n\ts_name\nLIMIT 1"

    println(s"Q20:\n $sql")
    sqlContext.sql(sql)
  }

}
