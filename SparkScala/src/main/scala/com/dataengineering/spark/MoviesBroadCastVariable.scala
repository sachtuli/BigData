package com.dataengineering.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object MoviesBroadCastVariable {
  val startTimeMillis: Long = System.currentTimeMillis()
  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1")

    // Create a Map of Ints to Strings
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MoviesBroadCastVariable")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    // Load up movie data as dataset
    import spark.implicits._
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    // Get number of reviews per movieID
    val movieCounts = movies.groupBy("movieID").count()

    // Create a user-defined function to look up movie names from our
    // broadCasted Map variable.
    // to define our UDF to use in Dataset. (InLine func)

    val lookupName : Int => String = (movieID:Int)=>{
      nameDict.value(movieID)
    }

    // Then wrap it with a udf
    val lookupNameUDF = udf(lookupName)

    // Add a movieTitle column using our new udf
    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

    // Sort the results
    val sortedMoviesWithNames = moviesWithNames.sort(desc("count"))

    // Show the results without truncating it
    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)
    val endTimeMillis: Long = System.currentTimeMillis()
    val duration: Number = (endTimeMillis - startTimeMillis) / 1000
    print("Duration: " , f"$duration sec")
  }
}

