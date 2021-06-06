package com.dataengineering.spark

import org.apache.log4j._
import org.apache.spark._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object CustomerSpentAnalysisRDD {
  
  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerSpentAnalysisRDD")
    
    val input = sc.textFile("data/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)
    
    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )
    
    val flipped = totalByCustomer.map( x => (x._2, x._1) )
    
    val totalByCustomerSorted = flipped.sortByKey()  //min to max amount
    
    val results = totalByCustomerSorted.collect()
    
    // Print the results.
    results.foreach(x => println("CustomerId: " +x._2, "TotalAmount: " +x._1 ))
  }
  
}

