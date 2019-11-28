package poc.spark.deltas.shared

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SparkSession }
import zio.ZIO

sealed trait DeltalakeLevel {
  def name: String
}

object DeltalakeLevel {
  case object Bronze extends DeltalakeLevel {
    override val name: String = "bronze"
  }
  case object Silver extends DeltalakeLevel {
    override val name: String = "silver"
  }
  case object Gold extends DeltalakeLevel {
    override val name: String = "gold"
  }
}

////

trait Deltalake {
  def deltalake: Deltalake.Service
}

object Deltalake {
  trait DeltalakeError
  final case class ExceptionEncountered(exception: Throwable) extends DeltalakeError

  trait Service {
    def basePath(level: DeltalakeLevel): String
    def topicBasePath(level: DeltalakeLevel, topicName: String): String
    def topicDataPath(level: DeltalakeLevel, topicName: String): String
    def topicCheckpointPath(level: DeltalakeLevel, topicName: String): String
    def batchDataframe(spark: SparkSession, log: Log, path: String): ZIO[Any, Throwable, DataFrame]
    def streamingDataframe(spark: SparkSession, path: String): ZIO[Any, Throwable, DataFrame]
    def setupBatchTopicTableDataframe(
      spark: SparkSession,
      log: Log,
      topicName: String
    ): ZIO[Any, Throwable, DataFrame]
    def removeAt(path: String): ZIO[Any, Throwable, Unit]
  }

  ////

  trait Live extends Deltalake {
    override def deltalake: Service =
      new Service {
        // TODO configurable
        override def basePath(level: DeltalakeLevel): String =
          //    s"/Users/s117476/temp/testdata/deltalake/${level.name}/topic"
          s"s3a://poc-minibatch-deltalake/deltalake/${level.name}/region"

        override def topicBasePath(level: DeltalakeLevel, topicName: String): String =
          s"${basePath(level)}/$topicName"

        override def topicDataPath(level: DeltalakeLevel, topicName: String): String =
          s"${topicBasePath(level, topicName)}/data"

        override def topicCheckpointPath(level: DeltalakeLevel, topicName: String): String =
          s"${topicBasePath(level, topicName)}/_checkpoints"

        ////

        override def batchDataframe(
          spark: SparkSession,
          log: Log,
          path: String
        ): ZIO[Any, Throwable, DataFrame] =
          for {
            _ <- log.info(s"Loading dataframe $path")
            df <- ZIO.effect {
              spark
                .sqlContext
                .read
                .format("delta")
                .load(path)
            }
          } yield df

        override def streamingDataframe(
          spark: SparkSession,
          path: String
        ): ZIO[Any, Throwable, DataFrame] =
          ZIO.effect {
            spark
              .readStream
              .format("delta")
              .load(path)
          }

        override def setupBatchTopicTableDataframe(
          spark: SparkSession,
          log: Log,
          topicName: String
        ): ZIO[Any, Throwable, DataFrame] =
          for {
            df <- batchDataframe(spark, log, topicDataPath(DeltalakeLevel.Silver, topicName))
            dfDeletesApplied <- ZIO.effect(df.filter(col("value").isNotNull)) // emulate CDC delete
            columnNames <- valueColumnNames(dfDeletesApplied) // index of "value" field in (key, value)
            columns <- ZIO.effect(columnNames.map(colname => col(s"value.$colname")))
            dfTable <- ZIO.effect(dfDeletesApplied.select(columns: _*))
            _ <- ZIO.effect(dfTable.createOrReplaceTempView(topicName))
          } yield dfTable

        private def valueColumnNames(df: DataFrame): ZIO[Any, Throwable, List[String]] =
          ZIO.effect {
            df.schema.fields(1).dataType match {
              case StructType(fields) => fields.map(_.name).toList
              case _ =>
                throw new RuntimeException(df.schema.fields.toString) // TODO handle properly
            }
          }

        override def removeAt(path: String): ZIO[Any, Throwable, Unit] =
          for {
            table <- ZIO.effect(DeltaTable.forPath(path))
            _ <- ZIO.effect(table.delete())
            _ <- ZIO.effect(table.vacuum().collect())
          } yield ()
      }

    object Live extends Live
  }

  object TestDeltalake {
    trait Test extends Deltalake {
      override def deltalake: Deltalake.Service =
        new Deltalake.Service {
          override def basePath(level: DeltalakeLevel): String =
            ???

          override def topicBasePath(level: DeltalakeLevel, topicName: String): String =
            ???

          override def topicDataPath(level: DeltalakeLevel, topicName: String): String =
            ???

          override def topicCheckpointPath(level: DeltalakeLevel, topicName: String): String =
            ???

          override def batchDataframe(
            spark: SparkSession,
            log: Log,
            path: String
          ): ZIO[Any, Throwable, DataFrame] =
            ???

          override def streamingDataframe(
            spark: SparkSession,
            path: String
          ): ZIO[Any, Throwable, DataFrame] =
            ???

          override def setupBatchTopicTableDataframe(
            spark: SparkSession,
            log: Log,
            topicName: String
          ): ZIO[Any, Throwable, DataFrame] =
            ???

          override def removeAt(path: String): ZIO[Any, Throwable, Unit] =
            ???
        }
    }

    object Test extends Test
  }
}
