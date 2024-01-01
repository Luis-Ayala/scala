package learning.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object TotalSpentByCustomerDataset:

  private case class CustomerOrder(customerID: Int, item: Int, amountSpent: Float)

  @main
  def totalSpentByCustomerDatasetMain(): Unit =
    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder.appName("TotalSpentByCustomerDataset").master("local[*]").getOrCreate()

    val temperatureSchema = new StructType()
      .add("customerID",  IntegerType,  nullable = true)
      .add("item",        IntegerType,  nullable = true)
      .add("amountSpent", FloatType,    nullable = true)

    import scala3encoders.given
    val ds = spark.read.schema(temperatureSchema).csv("data/customer-orders.csv").as[CustomerOrder]

    val totalSpentByCustomer = ds.groupBy(ds("customerID")).agg(round(sum(ds("amountSpent")), 2).alias("totalSpent"))
    val sortedTotalSpentByCustomer = totalSpentByCustomer.sort("totalSpent")
    sortedTotalSpentByCustomer.foreach(i => println(i))

    spark.stop()