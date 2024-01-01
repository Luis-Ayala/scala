package learning.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSortedDataset: 

  private case class Book(value: String)

  /** Our main function where the action happens */
  @main def wordCountBetterSortedDatasetMain(): Unit =
    
    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // Read each line of my book into an Dataset
    import scala3encoders.given
    val input = spark.read.text("data/book.txt").as[Book]

    // Split using a regular expression that extracts words
    val words = input
      .select(explode(split(input("value"), "\\W+")).alias("word"))
      .filter(input("value") =!= "")

    // Normalize everything to lowercase
    val lowercaseWords = words.select(lower(words("word")).alias("word"))

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords.groupBy("word").count()

    // Sort by counts
    val wordCountsSorted = wordCounts.sort("count")

    // Show the results.
    wordCountsSorted.show(wordCountsSorted.count.toInt)
    
    // ANOTHER WAY TO DO IT (Blending RDD's and Datasets)
    val bookRDD = spark.sparkContext.textFile("data/book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = spark.createDataset(wordsRDD)

    val lowercaseWordsDS = wordsDS.select(lower(wordsDS("value")).alias("word"))
    val wordCountsDS = lowercaseWordsDS.groupBy("word").count()
    val wordCountsSortedDS = wordCountsDS.sort("count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)
    
    spark.stop()