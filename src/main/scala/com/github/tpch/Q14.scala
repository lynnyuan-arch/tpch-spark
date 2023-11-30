package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 14
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q14 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    /**import schemaProvider._
    import sqlContext.implicits._

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))**/


    val sql = "select\n\t100.00 * sum(case\n\t\twhen p_type like 'PROMO%'\n\t\t\tthen l_extendedprice * (1 - l_discount)\n\t\telse 0\n\tend) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\nfrom\n\tlineitem,\n\tpart\nwhere\n\tl_partkey = p_partkey\n\tand l_shipdate >= date '1996-08-01'\n\tand l_shipdate < date '1996-08-01' + interval '1' month\nLIMIT 1"

    println(s"Q14:\n $sql")
    sqlContext.sql(sql)
  }

}
