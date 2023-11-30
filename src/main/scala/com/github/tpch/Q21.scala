package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{count, countDistinct, max}

/**
 * TPC-H Query 21
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q21 extends TpchQuery {

  override def execute(sqlContext: SQLContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
/*
    import schemaProvider._
    import sqlContext.implicits._

    val fsupplier = supplier.select($"s_suppkey", $"s_nationkey", $"s_name")

    val plineitem = lineitem.select($"l_suppkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")
    //cache

    val flineitem = plineitem.filter($"l_receiptdate" > $"l_commitdate")
    // cache

    val line1 = plineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val line2 = flineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val forder = orders.select($"o_orderkey", $"o_orderstatus")
      .filter($"o_orderstatus" === "F")

    nation.filter($"n_name" === "SAUDI ARABIA")
      .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
      .join(flineitem, $"s_suppkey" === flineitem("l_suppkey"))
      .join(forder, $"l_orderkey" === forder("o_orderkey"))
      .join(line1, $"l_orderkey" === line1("key"))
      .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
      .select($"s_name", $"l_orderkey", $"l_suppkey")
      .join(line2, $"l_orderkey" === line2("key"), "left_outer")
      .select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
      .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")
      .groupBy($"s_name")
      .agg(count($"l_suppkey").as("numwait"))
      .sort($"numwait".desc, $"s_name")
      .limit(100)
    */


    val sql = "select\n\ts_name,\n\tcount(*) as numwait\nfrom\n\tsupplier,\n\tlineitem l1,\n\torders,\n\tnation\nwhere\n\ts_suppkey = l1.l_suppkey\n\tand o_orderkey = l1.l_orderkey\n\tand o_orderstatus = 'F'\n\tand l1.l_receiptdate > l1.l_commitdate\n\tand exists (\n\t\tselect\n\t\t\t*\n\t\tfrom\n\t\t\tlineitem l2\n\t\twhere\n\t\t\tl2.l_orderkey = l1.l_orderkey\n\t\t\tand l2.l_suppkey <> l1.l_suppkey\n\t)\n\tand not exists (\n\t\tselect\n\t\t\t*\n\t\tfrom\n\t\t\tlineitem l3\n\t\twhere\n\t\t\tl3.l_orderkey = l1.l_orderkey\n\t\t\tand l3.l_suppkey <> l1.l_suppkey\n\t\t\tand l3.l_receiptdate > l3.l_commitdate\n\t)\n\tand s_nationkey = n_nationkey\n\tand n_name = 'UNITED STATES'\ngroup by\n\ts_name\norder by\n\tnumwait desc,\n\ts_name\nLIMIT 100"

    println(s"Q21:\n $sql")
    sqlContext.sql(sql)
  }

}
