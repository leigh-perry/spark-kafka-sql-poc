import Dependencies._
import sbt.Keys.crossScalaVersions

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

// NOTE: When a submodule or project is created, add it to the below aggregate list, so `sbt test` tests everything
lazy val root = (project in file("."))
  .aggregate(
    `kafka-spark-deltas`,
    `abris-patch`,
  )
  .settings(
    crossScalaVersions := Nil,
    publish / skip := true
  )

lazy val `kafka-spark-deltas` =
  (project in file("apps/kafka-spark-deltas"))
    .disablePlugins(SbtAvro)
    .dependsOn(`abris-patch`)
    .settings(
      resolvers += "confluent" at "https://packages.confluent.io/maven/",
      libraryDependencies ++=
        Seq(
          deltaLake,
          kafkaStreams,
          kafkaStreamsAvroSerde,
          sparkCore % "provided",
          sparkSql % "provided",
          sparkSqlKafka,
          cats,
          catsEffect,
          zio,
          ziocats,
          abris,
          logback,
          specs2Core % "test"
        )
    )
    .settings(
      // Apache Spark uses jackson 2.6.7 version:
      // Whereas Kafka uses Jackson jackson: "2.9.x" version.
      // resulting in: Caused by: com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.9.7
      scalaVersion := "2.11.12",
      dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
    )
    .settings(
      sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue
    )

////

lazy val `abris-patch` = (project in file("modules/abris-patch"))
  .settings(
    name := "abris-patch",
    libraryDependencies ++=
      Seq(
        sparkCore % "provided",
        sparkSql % "provided",
        sparkSqlKafka,
        abris
      ),
    scalaVersion := scala211
  )

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
