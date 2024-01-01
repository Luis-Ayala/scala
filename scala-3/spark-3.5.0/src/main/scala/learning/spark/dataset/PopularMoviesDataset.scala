package learning.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object PopularMoviesDataset:

  final private case class Movie(movieID: Int)

  @main
  def popularMoviesDatasetMain(): Unit =
    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder.appName("PopularMoviesDataset").master("local[*]").getOrCreate()

    val movieSchema = new StructType()
      .add("userID",    IntegerType,  nullable = true)
      .add("movieID",   IntegerType,  nullable = true)
      .add("rating",    IntegerType,  nullable = true)
      .add("timestamp", LongType,     nullable = true)
    // Load up movie data as dataset
    import scala3encoders.given
    val ds = spark.read.option("sep", "\t").schema(movieSchema).csv("data/ml-100k/u.data").as[Movie]

    ds.groupBy("movieID").count().orderBy(desc("count")).show(10)

    spark.stop()