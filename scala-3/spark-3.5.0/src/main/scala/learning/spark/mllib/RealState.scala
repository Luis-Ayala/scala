package learning.spark.mllib

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.*
import org.apache.spark.sql.types.*

object RealState:

  private case class RegressionSchema(No: Int, TransactionDate: Double, HouseAge: Double,
                                      DistanceToMRT: Double, NumberConvenienceStores: Int, Latitude: Double,
                                      Longitude: Double, PriceOfUnitArea: Double)

  @main
  def realStateMain(): Unit =

    val spark = SparkSession
      .builder
      .appName("DecisionTreeRegressor")
      .master("local[*]")
      .getOrCreate()

    import scala3encoders.given
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/realestate.csv")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("HouseAge", "DistanceToMRT", "NumberConvenienceStores")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
      .select("PriceOfUnitArea", "features")

    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    val lir = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")

    val model = lir.fit(trainingDF)

    val fullPredictions = model.transform(testDF).cache()

    val predictionAndLabel = fullPredictions.select("prediction", "PriceOfUnitArea").collect()

    for (prediction <- predictionAndLabel)
      println(prediction)

    spark.stop()
