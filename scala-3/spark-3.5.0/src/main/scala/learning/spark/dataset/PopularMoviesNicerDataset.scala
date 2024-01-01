package learning.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesNicerDataset:

  private case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Load up a Map of movie IDs to movie names. */
  private def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    lines.getLines()
      .map(line => line.split('|'))
      .filter(_.length > 1)
      .foreach(fields => movieNames += fields(0).toInt -> fields(1))

    lines.close()

    movieNames
  }

  /** Our main function where the action happens */
  @main
  def popularMoviesNicerDatasetMain(): Unit =
    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder.appName("PopularMoviesNicer").master("local[*]").getOrCreate()
    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID",    IntegerType,  nullable = true)
      .add("movieID",   IntegerType,  nullable = true)
      .add("rating",    IntegerType,  nullable = true)
      .add("timestamp", LongType,     nullable = true)

    // Load up movie data as dataset
    import scala3encoders.given
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    // Get number of reviews per movieID
    val movieCounts = movies.groupBy("movieID").count()

    // Create a user-defined function to look up movie names from our
    // shared Map variable.

    // We start by declaring an "anonymous function" in Scala
    val lookupName : Int => String = (movieID:Int)=>{
      nameDict.value(movieID)
    }

    // Then wrap it with a udf
    import scala3udf.Udf as udf 
    val lookupNameUDF = udf(lookupName)

    // Add a movieTitle column using our new udf
    val moviesWithNames = movieCounts.select(movieCounts("movieID"), movieCounts("count"), lookupNameUDF(col("movieID")).alias("movieTitle"))

    // Sort the results
    val sortedMoviesWithNames = moviesWithNames.sort("count")

    // Show the results without truncating it
    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)