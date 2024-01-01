package learning.spark.hello

import org.apache.log4j.*
import org.apache.spark.*

object HelloSpark:

  @main def helloSparkMain(): Unit =
    val sc = new SparkContext("local[*]", "HelloWorld")
    val lines = sc.textFile("data/ml-100k/u.data")
    val numLines = lines.count()
    println("Hello world! The u.data file has " + numLines + " lines.")
    sc.stop()