package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.min

/**
 * TPC-H Query 2
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q02 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**
    import schemaProvider._
    import sqlContext.implicits._

    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)
    **/
    val sql  = "select\n\ts_acctbal,\n\ts_name,\n\tn_name,\n\tp_partkey,\n\tp_mfgr,\n\ts_address,\n\ts_phone,\n\ts_comment\nfrom\n\tpart,\n\tsupplier,\n\tpartsupp,\n\tnation,\n\tregion\nwhere\n\tp_partkey = ps_partkey\n\tand s_suppkey = ps_suppkey\n\tand p_size = 15\n\tand p_type like '%BRASS'\n\tand s_nationkey = n_nationkey\n\tand n_regionkey = r_regionkey\n\tand r_name = 'EUROPE'\n\tand ps_supplycost = (\n\t\tselect\n\t\t\tmin(ps_supplycost)\n\t\tfrom\n\t\t\tpartsupp,\n\t\t\tsupplier,\n\t\t\tnation,\n\t\t\tregion\n\t\twhere\n\t\t\tp_partkey = ps_partkey\n\t\t\tand s_suppkey = ps_suppkey\n\t\t\tand s_nationkey = n_nationkey\n\t\t\tand n_regionkey = r_regionkey\n\t\t\tand r_name = 'EUROPE'\n\t)\norder by\n\ts_acctbal desc,\n\tn_name,\n\ts_name,\n\tp_partkey \n\t limit 100"
    println(s"Q02:\n $sql")
    sqlContext.sql(sql)
  }

}
