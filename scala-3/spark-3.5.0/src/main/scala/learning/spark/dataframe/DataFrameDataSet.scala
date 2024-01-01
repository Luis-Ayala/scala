package learning.spark.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameDataSet:
  private case class Person(id: Int, name: String, age: Int, friends: Int)

  @main def dataFrameDataSetMain(): Unit =
    val spark = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()

    import scala3encoders.given
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    people.printSchema()

    people.select("name").show()

    people.filter(people("age") < 21).show()

    people.groupBy("age").count().show()

    people.select(people("name"), people("age") + 10).show()

    spark.stop()