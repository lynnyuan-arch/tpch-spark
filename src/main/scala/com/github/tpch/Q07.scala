package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 7
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q07 extends TpchQuery {

  override def execute(sqlContext: SQLContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._
    **/
//    val getYear = udf { (x: String) => x.substring(0, 4) }
//    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//
//    // cache fnation
//
//    val fnation = nation.filter($"n_name" === "FRANCE" || $"n_name" === "GERMANY")
//    val fline = lineitem.filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")
//
//    val supNation = fnation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
//      .join(fline, $"s_suppkey" === fline("l_suppkey"))
//      .select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate")
//
//    fnation.join(customer, $"n_nationkey" === customer("c_nationkey"))
//      .join(order, $"c_custkey" === order("o_custkey"))
//      .select($"n_name".as("cust_nation"), $"o_orderkey")
//      .join(supNation, $"o_orderkey" === supNation("l_orderkey"))
//      .filter($"supp_nation" === "FRANCE" && $"cust_nation" === "GERMANY"
//        || $"supp_nation" === "GERMANY" && $"cust_nation" === "FRANCE")
//      .select($"supp_nation", $"cust_nation",
//        getYear($"l_shipdate").as("l_year"),
//        decrease($"l_extendedprice", $"l_discount").as("volume"))
//      .groupBy($"supp_nation", $"cust_nation", $"l_year")
//      .agg(sum($"volume").as("revenue"))
//      .sort($"supp_nation", $"cust_nation", $"l_year")
/*
    val sql  =
"""select supp_nation, cust_nation, l_year, sum(volume) as revenue
  |from (
  |   select n1.n_name as supp_nation,
  |          n2.n_name as cust_nation,
  |          extract(year from l_shipdate) as l_year,
  |          l_extendedprice * (1 - l_discount) as volume
  |   from supplier, lineitem, orders, customer, nation n1, nation n2
  |   where s_suppkey = l_suppkey
  |         and o_orderkey = l_orderkey
  |         and c_custkey = o_custkey
  |         and s_nationkey = n1.n_nationkey
  |         and c_nationkey = n2.n_nationkey
  |         and ((n1.n_name = 'BRAZIL' and n2.n_name = 'FRANCE') or (n1.n_name = 'FRANCE' and n2.n_name = 'BRAZIL'))
  |         and l_shipdate between date '1995-01-01' and date '1996-12-31'
  |) as shipping
  |group by supp_nation, cust_nation, l_year
  |order by supp_nation, cust_nation, l_year
  |limit 4""".stripMargin*/

    val sql = "select\n\tsupp_nation,\n\tcust_nation,\n\tl_year,\n\tsum(volume) as revenue\nfrom\n\t(\n\t\tselect\n\t\t\tn1.n_name as supp_nation,\n\t\t\tn2.n_name as cust_nation,\n\t\t\tyear(l_shipdate) as l_year,\n\t\t\tl_extendedprice * (1 - l_discount) as volume\n\t\tfrom\n\t\t\tsupplier,\n\t\t\tlineitem,\n\t\t\torders,\n\t\t\tcustomer,\n\t\t\tnation n1,\n\t\t\tnation n2\n\t\twhere\n\t\t\ts_suppkey = l_suppkey\n\t\t\tand o_orderkey = l_orderkey\n\t\t\tand c_custkey = o_custkey\n\t\t\tand s_nationkey = n1.n_nationkey\n\t\t\tand c_nationkey = n2.n_nationkey\n\t\t\tand (\n\t\t\t\t(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')\n\t\t\t\tor (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')\n\t\t\t)\n\t\t\tand l_shipdate between date '1995-01-01' and date '1996-12-31'\n\t) as shipping\ngroup by\n\tsupp_nation,\n\tcust_nation,\n\tl_year\norder by\n\tsupp_nation,\n\tcust_nation,\n\tl_year\nLIMIT 1"

    println(s"Q07:\n$sql")
    sqlContext.sql(sql)
  }

}
