import sbt._

object Dependencies {
  private object versions {
    val abris = "2.2.3"
    val avro = "1.8.2"
    val avroRandomGenerator = "0.2.1"
    val confluentPlatform = "5.3.1"
    val databricksSparkAvro = "4.0.0"
    val deltaLake = "0.3.0"
    val kafka = "2.3.0"
    val logback = "1.2.3"
    val spark = "2.4.3"
    val specs2 = "4.7.1"
    val zio = "1.0.0-RC12-1"
    val cats = "2.0.0"
    val catsEffect = "2.0.0"
    val ziocats = "2.0.0.0-RC3"
  }

  val abris = "za.co.absa" %% "abris" % versions.abris
  val avro = "org.apache.avro" % "avro" % versions.avro
  val avroConverter = "io.confluent" % "kafka-connect-avro-converter" % versions.confluentPlatform
  val avroRandomGenerator = "io.confluent.avro" % "avro-random-generator" % versions.avroRandomGenerator
  val confluentCommonConfig = "io.confluent" % "common-config" % versions.confluentPlatform
  val confluentCommonUtils = "io.confluent" % "common-utils" % versions.confluentPlatform
  val deltaLake = "io.delta" %% "delta-core" % versions.deltaLake
  val databricksSparkAvro = "com.databricks" %% "spark-avro" % versions.databricksSparkAvro
  val kafka = "org.apache.kafka" %% "kafka" % versions.kafka
  val kafkaAvroSerializer = "io.confluent" % "kafka-avro-serializer" % versions.confluentPlatform
  val kafkaClients = "org.apache.kafka" % "kafka-clients" % versions.kafka
  val kafkaStreams = "org.apache.kafka" % "kafka-streams" % versions.kafka
  val kafkaStreamsAvroSerde = "io.confluent" % "kafka-streams-avro-serde" % versions.confluentPlatform
  val kafkaStreamsScala = "org.apache.kafka" %% "kafka-streams-scala" % versions.kafka
  val kafkaStreamsTestUtils = "org.apache.kafka" % "kafka-streams-test-utils" % versions.kafka
  val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
  val sparkSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % versions.spark
  val specs2Core = "org.specs2" %% "specs2-core" % versions.specs2
  val specs2Matchers = "org.specs2" %% "specs2-matcher-extra" % versions.specs2
  val specs2Scalacheck = "org.specs2" %% "specs2-scalacheck" % versions.specs2
  val zio = "dev.zio" %% "zio" % versions.zio
  val zioStreams = "dev.zio" %% "zio-streams" % versions.zio
  val cats = "org.typelevel" %% "cats-core" % versions.cats
  val catsEffect = "org.typelevel" %% "cats-effect" % versions.catsEffect
  val ziocats = "dev.zio" %% "zio-interop-cats" % versions.ziocats
  val logback = "ch.qos.logback" % "logback-classic" % versions.logback

  def scalaReflect(version: String) =
    "org.scala-lang" % "scala-reflect" % version
}
