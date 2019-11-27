import sbt._

object versions {
  val absa = "2.2.2"
  val avro = "1.8.2"
  val confluent = "5.3.0"
  val deltaLake = "0.3.0"
  val jodatime = "2.9.9"
  val kafka = "2.3.0"
  val log4j = "2.11.2"
  val logback = "1.2.3"
  val scalaTest = "3.0.5"
  val spark = "2.4.3"
  val specs2 = "4.3.4"
  val cats = "2.0.0"
  val catsEffect = "2.0.0"
}

object Dependencies {
  val abris = "za.co.absa" %% "abris" % versions.absa
  val avro = "org.apache.avro" % "avro" % versions.avro
  val deltaLake = "io.delta" %% "delta-core" % versions.deltaLake
  val jodatime = "joda-time" % "joda-time" % versions.jodatime
  val kafkaStreams = "org.apache.kafka" % "kafka-streams" % versions.kafka
  val kafkaStreamsAvroSerde = "io.confluent" % "kafka-streams-avro-serde" % versions.confluent
  val log4j2Api = "org.apache.logging.log4j" % "log4j-api" % versions.log4j
  val log4j2Core = "org.apache.logging.log4j" % "log4j-core" % versions.log4j
  val log4jslf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % versions.log4j
  val logbackClassic = "ch.qos.logback" % "logback-classic" % versions.logback
  val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
  val sparkStream = "org.apache.spark" %% "spark-streaming-kafka-0-10" % versions.spark
  val sparkStreamSql = "org.apache.spark" %% "spark-sql-kafka-0-10" % versions.spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
  val specs2Core = "org.specs2" %% "specs2-core" % versions.specs2
  val cats = "org.typelevel" %% "cats-core" % versions.cats
  val catsEffect = "org.typelevel" %% "cats-effect" % versions.catsEffect
}
