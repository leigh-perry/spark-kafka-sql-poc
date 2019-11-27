import Dependencies._
import KafkaComposePlugin._
import sbtavro.SbtAvro

scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    scalaVersion := "2.11.12",
    crossScalaVersions := Nil,
    publish / skip := true
  )
  .aggregate(sparkKafkaSql)

lazy val sparkKafkaSql =
  WithKafkaSetUp {
    (project in file("apps/spark-kafka-sql"))
      .enablePlugins(PackagingTypePlugin)
      .disablePlugins(SbtAvro)
      .settings(
        resolvers += "confluent" at "https://packages.confluent.io/maven/",
        libraryDependencies ++=
          Seq(
            deltaLake,
            kafkaStreams,
            kafkaStreamsAvroSerde,
            sparkCore % "provided",
            sparkSql,
            sparkStreamSql,
            cats,
            catsEffect,
            abris,
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
      .settings(
        assemblyMergeStrategy in assembly := {
          case x if x.contains("BuildInfo") && !x.contains("poc") => MergeStrategy.discard
          case x if x.contains("UnusedStubClass") => MergeStrategy.first
          case x if x.contains("aopalliance") => MergeStrategy.first
          case x if x.contains("collections") => MergeStrategy.first
          case x if x.contains("yarn") => MergeStrategy.first
          case x if x.contains("inject") => MergeStrategy.first
          case x if x.contains("org.apache.arrow") => MergeStrategy.first
          case x if x.contains("git.properties") => MergeStrategy.first
          case x if x.contains("codegen-resources") => MergeStrategy.discard
          case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
          case x => MergeStrategy.defaultMergeStrategy(x)
        }
      )
  }

lazy val `avro-schemas` =
  (project in file("modules/avro-schemas"))
    .disablePlugins(SbtAvrohugger)
    .settings(
      scalaVersion := "2.11.12",
      (stringType in AvroConfig) := "String",
      (sourceDirectory in AvroConfig) := file("avro"),
      libraryDependencies ++= Seq(
        jodatime
      )
    )

lazy val WithKafkaSetUp: Project => Project =
  _
    .enablePlugins(DockerPlugin)
    .configs(IntegrationTest, KafkaTests)
    .settings(baseKafkaComposeSettings)
    .settings(inConfig(Test)(baseAssemblySettings))
    .settings(Defaults.itSettings)
    .settings(Seq(
      kafkaComposeTopicNames := List("xx", "yy"),
      kafkaComposeAppImageName := ImageName("test_images/kafka_spark_streaming_test"),
      kafkaComposeStreamingJobs := List(KStreamJob("poc.kafka.spark.test.Main", Map.empty))
    ): _*)
