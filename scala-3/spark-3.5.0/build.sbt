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
