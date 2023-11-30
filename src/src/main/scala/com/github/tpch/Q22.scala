package com.github.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{avg, count, sum, udf}

/**
 * TPC-H Query 22
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q22 extends TpchQuery {

  override def execute(sqlContext: SQLContext,  schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
/**
    import schemaProvider._
    import sqlContext.implicits._
**/
//    val sub2 = udf { (x: String) => x.substring(0, 2) }
//    val phone = udf { (x: String) => x.matches("13|31|23|29|30|18|17") }
//    val isNull = udf { (x: Any) => println(x); true }
//
//    val fcustomer = customer.select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))
//      .filter(phone($"cntrycode"))
//
//    val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
//      .agg(avg($"c_acctbal").as("avg_acctbal"))
//
//    order.groupBy($"o_custkey")
//      .agg($"o_custkey").select($"o_custkey")
//      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
//      //.filter("o_custkey is null")
//      .filter($"o_custkey".isNull)
//      .join(avg_customer)
//      .filter($"c_acctbal" > $"avg_acctbal")
//      .groupBy($"cntrycode")
//      .agg(count($"c_acctbal"), sum($"c_acctbal"))
//      .sort($"cntrycode")


    val sql = "select\n\tcntrycode,\n\tcount(*) as numcust,\n\tsum(c_acctbal) as totacctbal\nfrom\n\t(\n\t\tselect\n\t\t\tsubstring(c_phone from 1 for 2) as cntrycode,\n\t\t\tc_acctbal\n\t\tfrom\n\t\t\tcustomer\n\t\twhere\n\t\t\tsubstring(c_phone from 1 for 2) in\n\t\t\t\t('21', '22', '26', '16', '10', '11', '25')\n\t\t\tand c_acctbal > (\n\t\t\t\tselect\n\t\t\t\t\tavg(c_acctbal)\n\t\t\t\tfrom\n\t\t\t\t\tcustomer\n\t\t\t\twhere\n\t\t\t\t\tc_acctbal > 0.00\n\t\t\t\t\tand substring(c_phone from 1 for 2) in\n\t\t\t\t\t\t('21', '22', '26', '16', '10', '11', '25')\n\t\t\t)\n\t\t\tand not exists (\n\t\t\t\tselect\n\t\t\t\t\t*\n\t\t\t\tfrom\n\t\t\t\t\torders\n\t\t\t\twhere\n\t\t\t\t\to_custkey = c_custkey\n\t\t\t)\n\t) as custsale\ngroup by\n\tcntrycode\norder by\n\tcntrycode\nLIMIT 1"
    println(s"Q22:\n $sql")
    sqlContext.sql(sql)
  }
}
