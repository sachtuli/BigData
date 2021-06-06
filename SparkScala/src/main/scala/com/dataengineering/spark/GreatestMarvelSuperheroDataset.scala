package com.dataengineering.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/** Find the superhero with the most co-appearances. */
object GreatestMarvelSuperheroDataset {

  case class SuperHeroNames(id: Int, name: String)

  case class SuperHero(value: String)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MarvelUniverse")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]


    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1) // to subtract first column
      .groupBy("id").agg(sum("connections").alias("connections"))


    val theMostPopular = connections
      .sort($"connections".desc)
      .first()

    val mostPopularName = names
      .filter($"id" === theMostPopular(0))
      .select("name")
      .first()
    println(mostPopularName)

    println(s"${mostPopularName(0)} is the most popular superhero with ${theMostPopular(1)} co-appearances in Marvel Universe.")
    spark.stop()
  }
}
