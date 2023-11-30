package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{sum, udf}

/**
 * TPC-H Query 19
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q19 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
/*
    import schemaProvider._
    import sqlContext.implicits._

    val sm = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#12") &&
          sm($"p_container") &&
          $"l_quantity" >= 1 && $"l_quantity" <= 11 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#23") &&
            md($"p_container") &&
            $"l_quantity" >= 10 && $"l_quantity" <= 20 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
            (($"p_brand" === "Brand#34") &&
              lg($"p_container") &&
              $"l_quantity" >= 20 && $"l_quantity" <= 30 &&
              $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))
    */

    val sql = "select\n\tsum(l_extendedprice* (1 - l_discount)) as revenue\nfrom\n\tlineitem,\n\tpart\nwhere\n\t(\n\t\tp_partkey = l_partkey\n\t\tand p_brand = 'Brand#41'\n\t\tand p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n\t\tand l_quantity >= 2 and l_quantity <= 2 + 10\n\t\tand p_size between 1 and 5\n\t\tand l_shipmode in ('AIR', 'AIR REG')\n\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n\t)\n\tor\n\t(\n\t\tp_partkey = l_partkey\n\t\tand p_brand = 'Brand#53'\n\t\tand p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n\t\tand l_quantity >= 13 and l_quantity <= 13 + 10\n\t\tand p_size between 1 and 10\n\t\tand l_shipmode in ('AIR', 'AIR REG')\n\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n\t)\n\tor\n\t(\n\t\tp_partkey = l_partkey\n\t\tand p_brand = 'Brand#34'\n\t\tand p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n\t\tand l_quantity >= 24 and l_quantity <= 24 + 10\n\t\tand p_size between 1 and 15\n\t\tand l_shipmode in ('AIR', 'AIR REG')\n\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n\t)\nLIMIT 1"

    println(s"Q19:\n $sql")
    sqlContext.sql(sql)
  }

}
