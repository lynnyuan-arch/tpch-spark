package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 10
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q10 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val flineitem = lineitem.filter($"l_returnflag" === "R")

    orders.filter($"o_orderdate" < "1994-01-01" && $"o_orderdate" >= "1993-10-01")
      .join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
      .select($"c_custkey", $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc)
      .limit(20)**/
    val sql =
      """select
        |  c_custkey, c_name,
        |  sum(l_extendedprice * (1 - l_discount)) as revenue,
        |  c_acctbal, n_name, c_address, c_phone, c_comment
        |from customer, orders, lineitem, nation
        |where c_custkey = o_custkey
        |  and l_orderkey = o_orderkey
        |  and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month
        |  and l_returnflag = 'R'
        |  and c_nationkey = n_nationkey
        |  group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        |  order by revenue desc
        |  limit 20""".stripMargin


    println(s"Q10:\n $sql")
    sqlContext.sql(sql)
  }

}
