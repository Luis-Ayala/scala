package learning.spark.sql

import org.apache.spark.sql.SparkSession

object SparkSQLDataset:

  private case class Person(id: Int, name: String, age: Int, friends: Int)

  @main def sparkSqlMain(): Unit =
    val spark = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()

    import scala3encoders.given
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age between 13 AND 19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
