package poc.spark.deltas.app

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.applicative._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import poc.spark.deltas.shared.{App, DeltaLake, Logger, SparkApp}
import za.co.absa.abris.avro.AvroPatch._
import za.co.absa.abris.avro.read.confluent.SchemaManager

object BronzeIngestionJob
  extends IOApp {

  val kafkaBrokers = "localhost:9092"
  val schemaRegistryAddr = "http://localhost:8081"

  def run(args: List[String]): IO[ExitCode] =
    App.runProgram(program, "BRONZE")

  def program: Logger[IO] => IO[Unit] =
    log =>
      for {
        spark <- SparkApp.sparkLocal("SqlJoin")
          streamXx <- ingestionStream(spark, kafkaBrokers, schemaRegistryAddr, "xx")
          streamYy <- ingestionStream(spark, kafkaBrokers, schemaRegistryAddr, "yy")
          _ <- IO.delay(streamXx.awaitTermination())
          _ <- IO.delay(streamYy.awaitTermination())

      } yield ()


  def ingestionStream(
    spark: SparkSession,
    kafkaBrokers: String,
    schemaRegistryAddr: String,
    topicName: String
  ): IO[StreamingQuery] =
    for {
      schemaRegistryConfig <-
        Map(
          SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryAddr,
          SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topicName,
          SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
          SchemaManager.PARAM_KEY_SCHEMA_ID -> SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME,
          SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
          SchemaManager.PARAM_VALUE_SCHEMA_ID -> SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME
        ).pure[IO]
        query <-
          IO.delay {
            // Structured straming ETL query
            spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBrokers)
              .option("subscribe", topicName)
              .option("startingOffsets", "earliest")
              .load()
              .fromConfluentAvroForKey("key", schemaRegistryConfig)
              .fromConfluentAvroForValue("value", schemaRegistryConfig)
              .writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", DeltaLake.topicCheckpointPath(DeltaLake.Level.Bronze, topicName))
              .start(DeltaLake.topicDataPath(DeltaLake.Level.Bronze, topicName))
          }

    } yield query

}
