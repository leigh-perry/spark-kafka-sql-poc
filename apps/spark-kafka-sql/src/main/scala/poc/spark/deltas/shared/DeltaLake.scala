package poc.spark.deltas.shared

import cats.effect.IO
import cats.syntax.flatMap._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaLake {
  sealed trait Level {
    def name: String
  }
  object Level {
    case object Bronze extends Level {
      override val name: String = "bronze"
    }
    case object Silver extends Level {
      override val name: String = "silver"
    }
    case object Gold extends Level {
      override val name: String = "gold"
    }
  }

  // TODO configurable
  def basePath(level: Level): String =
    s"/Users/s117476/temp/testdata/deltalake/${level.name}/topic"

  def topicBasePath(level: Level, topicName: String): String =
    s"${basePath(level)}/$topicName"

  def topicDataPath(level: Level, topicName: String) =
    s"${topicBasePath(level, topicName)}/data"

  def topicCheckpointPath(level: Level, topicName: String) =
    s"${topicBasePath(level, topicName)}/_checkpoints"

  ////

  def batchDataframe(spark: SparkSession, log: Logger[IO], path: String): IO[DataFrame] =
    log.info(s"Loading dataframe $path") >>
      IO.delay {
        spark.sqlContext
          .read
          .format("delta")
          .load(path)
      }

  def streamingDataframe(spark: SparkSession, path: String): IO[DataFrame] =
    IO.delay {
      spark.readStream
        .format("delta")
        .load(path)
    }

  def setupBatchTopicTableDataframe(spark: SparkSession, log: Logger[IO], topicName: String): IO[DataFrame] =
    for {
      df <- DeltaLake.batchDataframe(spark, log, topicDataPath(Level.Silver, topicName))
        dfDeletesApplied <- IO.delay(df.filter(col("value").isNotNull)) // emulate CDC delete
        columnNames <- valueColumnNames(dfDeletesApplied) // index of "value" field in (key, value)
        columns <- IO.delay(columnNames.map(colname => col(s"value.$colname")))
        dfTable <- IO.delay(dfDeletesApplied.select(columns: _*))
        _ <- IO.delay(dfTable.createOrReplaceTempView(topicName))
    } yield dfTable

  private def valueColumnNames(df: DataFrame): IO[List[String]] =
    IO.delay {
      df.schema.fields(1).dataType match {
        case StructType(fields) => fields.map(_.name).toList
        case _ => throw new RuntimeException(df.schema.fields.toString) // TODO handle properly
      }
    }
}
