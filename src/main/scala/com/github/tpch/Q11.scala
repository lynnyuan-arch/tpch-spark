package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 11
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q11 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._

    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)**/

    val sql = "select\n\tps_partkey,\n\tsum(ps_supplycost * ps_availqty) as value\nfrom\n\tpartsupp,\n\tsupplier,\n\tnation\nwhere\n\tps_suppkey = s_suppkey\n\tand s_nationkey = n_nationkey\n\tand n_name = 'EGYPT'\ngroup by\n\tps_partkey having\n\t\tsum(ps_supplycost * ps_availqty) > (\n\t\t\tselect\n\t\t\t\tsum(ps_supplycost * ps_availqty) * 0.0001000000\n\t\t\tfrom\n\t\t\t\tpartsupp,\n\t\t\t\tsupplier,\n\t\t\t\tnation\n\t\t\twhere\n\t\t\t\tps_suppkey = s_suppkey\n\t\t\t\tand s_nationkey = n_nationkey\n\t\t\t\tand n_name = 'EGYPT'\n\t\t)\norder by\n\tvalue desc\nLIMIT 1"

    println(s"Q11:\n $sql")
    sqlContext.sql(sql)
  }

}
