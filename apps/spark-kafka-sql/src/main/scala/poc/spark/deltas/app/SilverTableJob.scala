package poc.spark.deltas.app

import cats.effect.{ExitCode, IO, IOApp}
import org.apache.spark.sql.functions.{col, last, struct}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Column, SparkSession}
import poc.spark.deltas.shared.DeltaLake.Level.{Bronze, Silver}
import poc.spark.deltas.shared.DeltaLake.{Level, topicBasePath, topicDataPath}
import poc.spark.deltas.shared.{App, DeltaLake, Logger, SparkApp}

object SilverTableJob
  extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    App.runProgram(program, "SILVER")

  def program: Logger[IO] => IO[Unit] =
    log =>
      for {
        spark <- SparkApp.sparkLocal("ToTable")
          streamXx <-
            tableStream(
              spark,
              "xx",
              // TODO generalise
              struct(
                col("value.userid").alias("userid"),
                col("value.name").alias("name"),
                col("value.gender").alias("gender"),
                col("value.regionid").alias("regionid")
              )
            )
          streamYy <-
            tableStream(
              spark,
              "yy",
              struct(
                col("value.userid").alias("userid"),
                col("value.refcount").alias("refcount"),
                col("value.optional").alias("optional")
              )
            )

          _ <- IO.delay(streamXx.awaitTermination())
          _ <- IO.delay(streamYy.awaitTermination())

      } yield ()

  private def tableStream(spark: SparkSession, topicName: String, columns: Column): IO[StreamingQuery] =
    for {
      df <- DeltaLake.streamingDataframe(spark, topicDataPath(Bronze, topicName))
        query <-
          IO.delay {
            df.groupBy("value.userid")
              .agg(last(columns, ignoreNulls = false).alias("value"))
              .writeStream
              .format("delta")
              .outputMode(OutputMode.Complete)
              .option("checkpointLocation", s"${topicBasePath(Silver, topicName)}/_checkpoints")
              .start(topicDataPath(Level.Silver, topicName))
          }
    } yield query
}
