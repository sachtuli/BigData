package com.dataengineering.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object CustomerSpentAnalysisDataset {
  case class customerOrders(cust_id: Int, item_id: Int, amount_spent: Double)
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("CustomerSpentAnalysisDataset")
      .master("local[*]").getOrCreate()

    val customerOrdersSchema = new StructType()
      .add("cust_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)

    import spark.implicits._
    val ds = spark.read.schema(customerOrdersSchema).csv("data/customer-orders.csv").as[customerOrders]
    val groupedDS = ds.groupBy("cust_id").agg(round(sum("amount_spent"), 2)
      .alias("total_spent"))
    val sortedDS = groupedDS.sort("cust_id")
    sortedDS.show(sortedDS.count().toInt)
    spark.stop()
  }
}
