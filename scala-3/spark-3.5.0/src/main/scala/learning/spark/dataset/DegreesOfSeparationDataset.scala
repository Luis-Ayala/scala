package learning.spark.dataset

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.{Dataset, SparkSession}

/** Finds the degrees of separation between two Marvel comic book characters, based
 *  on co-appearances in a comic.
 */
object DegreesOfSeparationDataset:

  // The characters we want to find the separation between.
  val startCharacterID = 5306 //SpiderMan
  val targetCharacterID = 14 //ADAM 3,031 (who?)

  private case class SuperHero(value: String)
  private case class BFSNode(id: Int, connections: Array[Int], distance: Int, color: String)

  /** Create "iteration 0" of our RDD of BFSNodes */
  private def createStartingDs(spark:SparkSession): Dataset[BFSNode] =
    import scala3encoders.given
    val inputFile = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    // Parse the data such as first element will be in column id and all the rest will be in second column as Array
    val connections = inputFile
      .withColumn("id", split(col("value"), " ")(0).cast("int"))
      .withColumn("connections", slice(split(col("value"), " "), 2, 9999).cast("array<int>"))
      .select("id", "connections")

    // Add distance and color columns
    val result = connections
      .withColumn("distance",
        when(col("id") === startCharacterID,0)
          .when(col("id") =!= startCharacterID,9999))
      .withColumn("color",
        when(col("id") === startCharacterID,"GRAY")
          .when(col("id") =!= startCharacterID,"WHITE")).as[BFSNode]

    result

  private def exploreNode(spark:SparkSession, ds: Dataset[BFSNode], iteration: Int): (Dataset[BFSNode], Long) =
    import spark.implicits._
    // Get all node which needs to be explored
    val rawExploreDS = ds
      .filter($"color" === "GRAY")
      .select($"id", explode($"connections").alias("child")).distinct()

    val hitCount = rawExploreDS.filter($"child" === targetCharacterID).count()
    val exploreDS = rawExploreDS.distinct().select("child")

    // All parent become explored after getting exploreDS so we marked these as "BLACK"
    import scala3encoders.given
    val exploring = ds
      .withColumn("color",
        when(col("color") === "GRAY","BLACK")
          .otherwise($"color")).as[BFSNode]

    // Mark all explored nodes on this iteration which were not previously explored and set distance
    val result = exploring
      .join(exploreDS, exploring("color") === "WHITE" && exploring("id") === exploreDS("child"), "leftouter")
      .withColumn("distance",
        when(col("child").isNotNull, iteration)
          .otherwise($"distance"))
      .withColumn("color",
        when(col("color") === "WHITE" && col("child").isNotNull, "GRAY")
          .otherwise($"color"))
      .select("id", "connections", "distance", "color").as[BFSNode]

    (result, hitCount)
  /** Our main function where the action happens */
  @main
  def degreesOfSeparationDatasetMain(): Unit =

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("degreesOfSeparationDataset")
      .master("local[*]")
      .getOrCreate()

    // character in our BFS traversal.
    var hitCount: Long = 0

    // Build dataset
    var iterationDs = createStartingDs(spark)

    for (iteration <- 1 to 10)
      println("Running BFS Iteration# " + iteration)
      val resultExplore = exploreNode(spark, iterationDs, iteration)
      iterationDs = resultExplore._1
      hitCount += resultExplore._2

      if (hitCount > 0)
        println("Hit the target character! From " + hitCount + " different direction(s).")
        System.exit(0)