package learning.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinTemperaturesDataset:

  private case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  @main
  def minTemperaturesDatasetMain(): Unit =
    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder.appName("MinTemperatures").master("local[*]").getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID",     StringType,   nullable = true)
      .add("date",          IntegerType,  nullable = true)
      .add("measure_type",  StringType,   nullable = true)
      .add("temperature",   FloatType,    nullable = true)

    import scala3encoders.given
    val ds = spark.read.schema(temperatureSchema).csv("data/1800.csv").as[Temperature]

    val minTemps = ds.filter(ds("measure_type") === "TMIN")

    val stationTemps = minTemps.select("stationID", "temperature")

    val minTempsByStation = stationTemps.groupBy("stationID").min("temperature")

    val minTempsByStationF = minTempsByStation.withColumn("temperature",
      round(minTempsByStation("min(temperature)") * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      .select("stationID", "temperature").sort("temperature")

    val results = minTempsByStationF.collect()

    results.foreach(i =>
      val station = i.get(0).asInstanceOf[String]
      val temp = i.get(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp"))

    spark.stop()
