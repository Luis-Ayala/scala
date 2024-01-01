# Spark 3.5.0

Spark examples using Scala 3.3.1, Spark 3.5.0 with Java 17
## Configuration
### VM parameters
```
-Xms512M
-Xmx1024M
-Xss1M
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
-Dlog4j.configurationFile=conf/log4j.properties
```

### SBT Task
![imagen](https://github.com/Luis-Ayala/scala/assets/7387143/5ee67c8e-4c6b-4ac2-a58e-a4c2fcdd406c)

### SBT file
```sh
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.1"
val sparkVersion = "3.5.0"
lazy val root = (project in file("."))
  .settings(
    name := "spark",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-core" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-mllib" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
      "io.github.vincenzobaz" %% "spark-scala3-encoders" % "0.2.5",
      "io.github.vincenzobaz" %% "spark-scala3-udf" % "0.2.5"
    )
  )
// include the 'provided' Spark dependency on the classpath for sbt run
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
```
### Output
```
"C:\Program Files\Eclipse Adoptium\jdk-17.0.7.7-hotspot\bin\java.exe" -Xms512M -Xmx1024M -Xss1M --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dlog4j.configurationFile=conf/log4j.properties -javaagent:C:\Users\luis_\Documents\java\ideaIC-2023.2.1.win\lib\idea_rt.jar=61099:C:\Users\luis_\Documents\java\ideaIC-2023.2.1.win\bin -Dfile.encoding=windows-1252 -classpath C:\Users\luis_\AppData\Roaming\JetBrains\IdeaIC2023.3\plugins\Scala\launcher\sbt-launch.jar xsbt.boot.Boot run
[info] welcome to sbt 1.9.7 (Eclipse Adoptium Java 17.0.7)
[info] loading global plugins from C:\Users\luis_\.sbt\1.0\plugins
[info] loading project definition from C:\Users\luis_\Documents\GitHub\scala\scala-3\spark-3.5.0\project
[info] loading settings for project root from build.sbt ...
[info] set current project to spark (in build file:/C:/Users/luis_/Documents/GitHub/scala/scala-3/spark-3.5.0/)
[info] compiling 19 Scala sources to C:\Users\luis_\Documents\GitHub\scala\scala-3\spark-3.5.0\target\scala-3.3.1\classes ...
[info] done compiling

Multiple main classes detected. Select one to run:
 [1] learning.spark.dataframe.dataFrameDataSetMain
 [2] learning.spark.dataset.degreesOfSeparationDatasetMain
 [3] learning.spark.dataset.friendsByAgeDatasetMain
 [4] learning.spark.dataset.minTemperaturesDatasetMain
 [5] learning.spark.dataset.mostObscureSuperheroDatasetMain
 [6] learning.spark.dataset.mostPopularSuperheroDatasetMain
 [7] learning.spark.dataset.movieSimilaritiesDatasetMain
 [8] learning.spark.dataset.popularMoviesDatasetMain
 [9] learning.spark.dataset.popularMoviesNicerDatasetMain
 [10] learning.spark.dataset.totalSpentByCustomerDatasetMain
 [11] learning.spark.dataset.wordCountBetterSortedDatasetMain
 [12] learning.spark.hello.helloScalaMain
 [13] learning.spark.hello.helloSparkMain
 [14] learning.spark.mllib.linearRegressionDataFrameDatasetMain
 [15] learning.spark.mllib.movieRecommendationsALSDatasetMain
 [16] learning.spark.mllib.realStateMain
 [17] learning.spark.rdd.degreesOfSeparationMain
 [18] learning.spark.rdd.sparkSqlMain
 [19] learning.spark.sql.sparkSqlMain

Enter number:
```
