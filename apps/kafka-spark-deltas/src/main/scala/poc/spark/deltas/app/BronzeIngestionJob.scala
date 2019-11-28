package poc.spark.deltas.app

import org.apache.spark.sql.streaming.StreamingQuery
import poc.spark.deltas.app.service.Bootstrap.ConfigSupport.Live
import poc.spark.deltas.app.service.Bootstrap.SparkSupport
import poc.spark.deltas.app.service.{ AppConfigError, AppRuntime }
import poc.spark.deltas.shared.DeltalakeLevel.Bronze
import poc.spark.deltas.shared.{ Deltalake, DeltalakeLevel, Log }
import za.co.absa.abris.avro.AvroPatch._
import za.co.absa.abris.avro.read.confluent.SchemaManager
import zio.blocking.Blocking
import zio.{ App, ZIO }

////

object BronzeIngestionJob extends App {
  sealed trait MainAppError
  final case class ConfigLoadError(message: AppConfigError) extends MainAppError
  final case class ExceptionEncountered(exception: Throwable) extends MainAppError

  override def run(args: List[String]): ZIO[Environment, Nothing, Int] =
    for {
      log <- Log.slf4j(prefix = "BRONZE")
      r <- resolvedProgram(log)
        .foldM(
          e => log.error(s"Application failed: $e") *> ZIO.succeed(1),
          _ => log.info("Application terminated with no error indication") *> ZIO.succeed(0)
        )
    } yield r

  def resolvedProgram(log: Log): ZIO[Any, MainAppError, Unit] =
    for {
      // Config is loaded separately in order to provide to `program`
      cfg <- Live.load.mapError(ConfigLoadError)
      sparkSession <- SparkSupport
        .Live
        .sparkSession("BRONZE")
        .mapError(ExceptionEncountered)
        .provide(Blocking.Live)
      resolved <- program.provide(
        new AppRuntime.All(cfg, log, sparkSession) with Deltalake.Live with Blocking.Live {}
      )
    } yield resolved

  def program: ZIO[
    AppRuntime.Config with AppRuntime.Logging with AppRuntime.Spark with Deltalake with Blocking,
    MainAppError,
    Unit
  ] =
    ZIO.accessM {
      env =>
        for {
          streamXx <- ingestionStream("xx")
          streamYy <- ingestionStream("yy")
          _ <- zio
            .blocking
            .effectBlocking(streamXx.awaitTermination())
            .mapError(ExceptionEncountered)
          _ <- zio
            .blocking
            .effectBlocking(streamYy.awaitTermination())
            .mapError(ExceptionEncountered)
        } yield ()
    }

  def ingestionStream(
    topicName: String
  ): ZIO[
    AppRuntime.Config with AppRuntime.Spark with Deltalake with Blocking,
    MainAppError,
    StreamingQuery
  ] =
    ZIO.accessM {
      env =>
        for {
          schemaRegistryConfig <- ZIO.succeed(
            Map(
              SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> env.cfg.schemaRegistryAddr,
              SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topicName,
              SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager
                .SchemaStorageNamingStrategies
                .TOPIC_NAME,
              SchemaManager.PARAM_KEY_SCHEMA_ID -> SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME,
              SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY ->
                SchemaManager
                  .SchemaStorageNamingStrategies
                  .TOPIC_NAME,
              SchemaManager.PARAM_VALUE_SCHEMA_ID -> SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME
            )
          )
          query <- zio
            .blocking
            .effectBlocking(
              // Structured streaming ETL query
              env
                .sparkSession
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", env.cfg.kafkaBrokers)
                .option("subscribe", topicName)
                .option("startingOffsets", "earliest")
                .load()
                .fromConfluentAvroForKey("key", schemaRegistryConfig)
                .fromConfluentAvroForValue("value", schemaRegistryConfig)
                .writeStream
                .format("delta")
                .outputMode("append")
                .option(
                  "checkpointLocation",
                  env.deltalake.topicCheckpointPath(Bronze, topicName)
                )
                .start(env.deltalake.topicDataPath(DeltalakeLevel.Bronze, topicName))
            )
            .mapError(ExceptionEncountered)
        } yield query
    }
}
