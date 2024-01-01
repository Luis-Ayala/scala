package learning.spark.mllib

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable
import scala.util.Random

object MovieRecommendationsALSDataset:

  private case class MoviesNames(movieId: Int, movieTitle: String)
  // Row format to feed into ALS
  private case class Rating(userID: Int, movieID: Int, rating: Float)

  // Get movie name by given dataset and id
  private def getMovieName(movieNames: Array[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(_.movieId == movieId)(0)

    result.movieTitle
  }
  /** Our main function where the action happens */
  @main
  def movieRecommendationsALSDatasetMain(): Unit =

    // Make a session
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local[*]")
      .getOrCreate()

    
    println("Loading movie names...")
    // Create schema when reading u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import scala3encoders.given
    // Create a broadcast dataset of movieID and movieTitle.
    // Apply ISO-885901 charset
    val names = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    val namesList = names.collect()

    // Load up movie data as dataset
    val ratings = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Rating]
    
    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")
    
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")
    
    val model = als.fit(ratings)
      
    // Get top-10 recommendations for the user we specified
    val userID:Int = Random.between(1,300).intValue()
    val users = spark.createDataset(Seq(userID)).toDF("userID")
    val recommendations = model.recommendForUserSubset(users, 10)
    
    // Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")


    for (userRecs <- recommendations)
      val myRecs = userRecs(1) // First column is userID, second is the recs
      val temp = myRecs.asInstanceOf[mutable.ArraySeq[Row]] // Tell Scala what it is
      for (rec <- temp)
        val movie = rec.getAs[Int](0)
        val movieName = getMovieName(namesList, movie)
        println(movieName)

    // Stop the session
    spark.stop()