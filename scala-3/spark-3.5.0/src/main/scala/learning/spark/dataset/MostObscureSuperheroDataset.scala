package learning.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostObscureSuperheroDataset:

  private case class SuperHeroNames(id: Int, name: String)
  private case class SuperHero(value: String)

  @main
  def mostObscureSuperheroDatasetMain(): Unit =
    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder.appName("MostObscureSuperheroDataset").master("local[*]").getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import scala3encoders.given
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
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))
    
    val minConnection = connections.agg(min(connections("connections"))).first().getLong(0)
      
    val join = names
      .join(connections)
      .where(names("id") === connections("id"))
      .where(connections("connections") === minConnection)
      .orderBy(names("name"))
    
    join.select("name").show()