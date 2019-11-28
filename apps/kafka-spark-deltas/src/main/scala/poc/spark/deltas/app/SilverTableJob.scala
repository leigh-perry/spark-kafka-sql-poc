package poc.spark.deltas.app

import poc.spark.deltas.app.service.{AppConfigError, AppRuntime, Bootstrap}
import poc.spark.deltas.shared.{Deltalake, DeltalakeLevel, Log}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, last, struct}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import poc.spark.deltas.app.service.{AppConfigError, AppRuntime}
import poc.spark.deltas.app.service.Bootstrap.ConfigSupport.Live
import poc.spark.deltas.app.service.Bootstrap.SparkSupport
import poc.spark.deltas.shared.DeltalakeLevel.{Bronze, Silver}
import poc.spark.deltas.shared.{Deltalake, DeltalakeLevel, Log}
import zio.blocking.Blocking
import zio.{App, ZIO}

object SilverTableJob extends App {
  sealed trait MainAppError
  final case class ConfigLoadError(message: AppConfigError) extends MainAppError
  final case class ExceptionEncountered(exception: Throwable) extends MainAppError

  override def run(args: List[String]): ZIO[Environment, Nothing, Int] =
    for {
      log <- Log.slf4j(prefix = "SILVER")
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
      sparkSession <- SparkSupport.Live
        .sparkSession("SILVER")
        .mapError(ExceptionEncountered)
        .provide(Blocking.Live) /// TODO alternatively have to remove Blocking requirement from spark bootstrap
      resolved <- program.provide(
        new AppRuntime.All(cfg, log, sparkSession) with Deltalake.Live with Blocking.Live {}
      )
    } yield resolved

  def program: ZIO[
    AppRuntime.Config with AppRuntime.Logging with Deltalake with AppRuntime.Spark,
    MainAppError,
    Unit
  ] =
    for {
      streamXx <- tableStream(
        "xx",
        // TODO generalise
        struct(
          col("value.userid").alias("userid"),
          col("value.name").alias("name"),
          col("value.gender").alias("gender"),
          col("value.regionid").alias("regionid")
        )
      )
      streamYy <- tableStream(
        "yy",
        struct(
          col("value.userid").alias("userid"),
          col("value.refcount").alias("refcount"),
          col("value.optional").alias("optional")
        )
      )

      _ <- ZIO.effect(streamXx.awaitTermination()).mapError(ExceptionEncountered)
      _ <- ZIO.effect(streamYy.awaitTermination()).mapError(ExceptionEncountered)
    } yield ()

  def tableStream(
    topicName: String,
    columns: Column
  ): ZIO[Deltalake with AppRuntime.Spark, MainAppError, StreamingQuery] =
    ZIO.accessM {
      env =>
        for {
          df <- env
            .deltalake
            .streamingDataframe(
              env.sparkSession,
              env.deltalake.topicDataPath(Bronze, topicName)
            )
            .mapError(ExceptionEncountered)
          path = s"${env.deltalake.topicBasePath(Silver, topicName)}/_checkpoints"
          query <- ZIO.effect {
            df.groupBy("value.userid")
              .agg(last(columns, ignoreNulls = false).alias("value"))
              .writeStream
              .format("delta")
              .outputMode(OutputMode.Complete)
              .option("checkpointLocation", path)
              .start(env.deltalake.topicDataPath(DeltalakeLevel.Silver, topicName))
          }.mapError(ExceptionEncountered)
        } yield query
    }
}
