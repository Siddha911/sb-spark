ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )

val sparkVersion = "3.4.3"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.14.2",
  "org.postgresql" % "postgresql" % "42.7.3",
  "joda-time" % "joda-time" % "2.12.7"
)