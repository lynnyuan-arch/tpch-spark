package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 8
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q08 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
   /** import schemaProvider._
    import sqlContext.implicits._
    **/
//    val getYear = udf { (x: String) => x.substring(0, 4) }
//    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//    val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }
//
//    val fregion = region.filter($"r_name" === "AMERICA")
//    val forder = order.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
//    val fpart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")
//
//    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
//
//    val line = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
//      decrease($"l_extendedprice", $"l_discount").as("volume")).
//      join(fpart, $"l_partkey" === fpart("p_partkey"))
//      .join(nat, $"l_suppkey" === nat("s_suppkey"))
//
//    nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
//      .select($"n_nationkey")
//      .join(customer, $"n_nationkey" === customer("c_nationkey"))
//      .select($"c_custkey")
//      .join(forder, $"c_custkey" === forder("o_custkey"))
//      .select($"o_orderkey", $"o_orderdate")
//      .join(line, $"o_orderkey" === line("l_orderkey"))
//      .select(getYear($"o_orderdate").as("o_year"), $"volume",
//        isBrazil($"n_name", $"volume").as("case_volume"))
//      .groupBy($"o_year")
//      .agg(sum($"case_volume") / sum("volume"))
//      .sort($"o_year")

    val sql = "select\n\to_year,\n\tsum(case\n\t\twhen nation = 'FRANCE' then volume\n\t\telse 0\n\tend) / sum(volume) as mkt_share\nfrom\n\t(\n\t\tselect\n\t\t\textract(year from o_orderdate) as o_year,\n\t\t\tl_extendedprice * (1 - l_discount) as volume,\n\t\t\tn2.n_name as nation\n\t\tfrom\n\t\t\tpart,\n\t\t\tsupplier,\n\t\t\tlineitem,\n\t\t\torders,\n\t\t\tcustomer,\n\t\t\tnation n1,\n\t\t\tnation n2,\n\t\t\tregion\n\t\twhere\n\t\t\tp_partkey = l_partkey\n\t\t\tand s_suppkey = l_suppkey\n\t\t\tand l_orderkey = o_orderkey\n\t\t\tand o_custkey = c_custkey\n\t\t\tand c_nationkey = n1.n_nationkey\n\t\t\tand n1.n_regionkey = r_regionkey\n\t\t\tand r_name = 'EUROPE'\n\t\t\tand s_nationkey = n2.n_nationkey\n\t\t\tand o_orderdate between date '1995-01-01' and date '1996-12-31'\n\t\t\tand p_type = 'MEDIUM PLATED TIN'\n\t) as all_nations\ngroup by\n\to_year\norder by\n\to_year\nLIMIT 1"
    println(s"Q08:\n $sql")
    sqlContext.sql(sql)
  }

}
