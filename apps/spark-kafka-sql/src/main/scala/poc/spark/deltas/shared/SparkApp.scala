package poc.spark.deltas.shared

import cats.effect.IO
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkApp {
  def sparkLocal(name: String): IO[SparkSession] =
    IO.delay {
      SparkSession.builder
        .appName(name)
        .master("local")
        .getOrCreate()
    }

  def executeSql(spark: SparkSession, sqlString: String): IO[DataFrame] =
    IO.delay(spark.sql(sqlString))
}
