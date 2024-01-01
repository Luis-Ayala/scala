package learning.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, round}

object FriendsByAgeDataset:
  private case class Person(id: Int, name: String, age: Int, friends: Int)

  @main def friendsByAgeDatasetMain(): Unit =
    val spark = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()

    import scala3encoders.given
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // Select only age and numFriends columns
    val friendsByAge = people.select("age", "friends")

    // From friendsByAge we group by "age" and then compute average
    friendsByAge.groupBy("age").avg("friends").show(5)

    // Sorted
    friendsByAge.groupBy("age").avg("friends").sort("age").show(5)

    // Formatted more nicely
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)).sort("age").show(5)

    // With a custom column name
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2).alias("friends_avg")).sort("age").show()

    spark.stop()
