package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{count, udf}

/**
 * TPC-H Query 13
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q13 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
   /** import schemaProvider._
    import sqlContext.implicits._

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    customer.join(orders, $"c_custkey" === orders("o_custkey")
      && !special(orders("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)**/


    val sql = "select\n\tc_count,\n\tcount(*) as custdist\nfrom\n\t(\n\t\tselect\n\t\t\tc_custkey,\n\t\t\tcount(o_orderkey)\n\t\tfrom\n\t\t\tcustomer left outer join orders on\n\t\t\t\tc_custkey = o_custkey\n\t\t\t\tand o_comment not like '%special%accounts%'\n\t\tgroup by\n\t\t\tc_custkey\n\t) as c_orders (c_custkey, c_count)\ngroup by\n\tc_count\norder by\n\tcustdist desc,\n\tc_count desc\nLIMIT 1"

    println(s"Q13:\n $sql")
    sqlContext.sql(sql)
  }

}
