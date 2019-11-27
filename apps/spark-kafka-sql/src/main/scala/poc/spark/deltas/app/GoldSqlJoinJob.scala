package poc.spark.deltas.app

import java.io.File
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import cats.effect.{ExitCode, IO, IOApp}
import cats.instances.list._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.parallel._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import poc.spark.deltas.shared.DeltaLake.Level.Gold
import poc.spark.deltas.shared.DeltaLake.topicDataPath
import poc.spark.deltas.shared.{App, DeltaLake, Logger, SparkApp}

object GoldSqlJoinJob
  extends IOApp {

  val namePrevious = "previousDF"
  val nameLatest = "latestDF"

  def run(args: List[String]): IO[ExitCode] = {
    // TODO make a program argument
    val requestedSqlString =
      """
        |SELECT x.userid, x.name, x.regionid, y.refcount, y.optional, x.gender
        |FROM xx x
        |INNER JOIN yy y
        |  ON x.userid = y.userid
        |""".stripMargin

    App.runProgram(program(requestedSqlString), "GOLD")
  }

  def program(sqlString: String): Logger[IO] => IO[Unit] =
    log =>
      for {
        spark <- SparkApp.sparkLocal("SqlJoin")
          pkName = "userid" // TODO generalise from parsed SQL (for topic/table names) etc
          timestamp <- IO.delay(Instant.now.atOffset(ZoneOffset.UTC))
          _ <- DeltaLake.setupBatchTopicTableDataframe(spark, log, "xx") // TODO infer from SQL
          _ <- DeltaLake.setupBatchTopicTableDataframe(spark, log, "yy") // TODO infer from SQL
          goldDir <- IO.delay(new File(DeltaLake.basePath(DeltaLake.Level.Gold)))
          snapshotNames = if (goldDir.exists) goldDir.listFiles.sorted.reverse.map(_.getName).toList else List.empty
          _ <- log.info(s"snapshotNames: $snapshotNames")
          _ <- emitResultChanges(spark, log, snapshotNames, pkName, timestamp, sqlString)
      } yield ()

  def emitResultChanges(
    spark: SparkSession,
    log: Logger[IO],
    snapshotNames: List[String],
    pkName: String,
    timestamp: OffsetDateTime,
    sqlString: String
  ): IO[Unit] =
    snapshotNames match {
      // First execution - all inserts
      case Nil =>
        for {
          _ <- log.info(s"First execution - no prior snapshots")
            dfLatest <- evaluateLatest(spark, log, sqlString)
            _ <- sendUpsertsToKafka(dfLatest, log)
            _ <- writeLatestSnapshot(dfLatest, log, pkName, timestamp)
        } yield ()

      // Subsequent executions - work out deltas as inserts, updates, deletes
      case prior :: history =>
        for {
          _ <- log.info(s"Most recent snapshot is $prior")
            dfLatest <- evaluateLatest(spark, log, sqlString)
            dfPrevious <- loadSnapshot(spark, log, prior)
            inserts <- leftOnly(spark, log, nameLatest, dfLatest, namePrevious, dfPrevious, pkName)
            updates <- differences(spark, log, dfPrevious, dfLatest, pkName)
            deletes <- leftOnly(spark, log, namePrevious, dfPrevious, nameLatest, dfLatest, pkName)
            _ <-
              (sendUpsertsToKafka(inserts, log), sendUpsertsToKafka(updates, log), sendDeletesToKafka(deletes, log))
                .parTupled
            // TODO remove partial snapshot if any failure... or use success flag??? bracket???
            _ <- writeLatestSnapshot(dfLatest, log, pkName, timestamp)
            _ <- removeDirs(history)
        } yield ()
    }

  def evaluateLatest(spark: SparkSession, log: Logger[IO], sqlString: String): IO[DataFrame] =
    for {
      sqlString <- IO.delay(sqlString)
        _ <- log.info(s"latest [$sqlString]")
        df <- SparkApp.executeSql(spark, sqlString)
        _ <- nameDataframe(df, log, nameLatest)
        _ <- log.info(s"Evaluated latest snapshot")
    } yield df

  def loadSnapshot(spark: SparkSession, log: Logger[IO], snapshotName: String): IO[DataFrame] =
    for {
      path <- DeltaLake.topicDataPath(DeltaLake.Level.Gold, snapshotName).pure[IO]
        df <- DeltaLake.batchDataframe(spark, log, path)
        _ <- nameDataframe(df, log, namePrevious)
        _ <- log.info(s"Loaded dataframe for $path")
    } yield df

  def leftOnly(
    spark: SparkSession,
    log: Logger[IO],
    lhsName: String,
    lhs: DataFrame,
    rhsName: String,
    rhs: DataFrame,
    pkName: String
  ): IO[DataFrame] =
    for {
      _ <- log.info("Schema for lhs:" + lhs.schema.fields.toList.toString())
        _ <- log.info("Schema for rhs:" + rhs.schema.fields.toList.toString())
        columnNames <- columnNames(lhs) // either lhs or rhs will do
        sqlString = leftOnlySql(lhsName, rhsName, pkName, columnNames)
        _ <- log.info(s"leftOnlySql [$sqlString]")
        df <- SparkApp.executeSql(spark, sqlString)
    } yield df

  def leftOnlySql(lhsName: String, rhsName: String, pkName: String, columnNames: List[String]): String =
    s"""
       |SELECT l.*
       |FROM $lhsName l
       |LEFT OUTER JOIN $rhsName r
       |  ON l.$pkName = r.$pkName
       |WHERE r.$pkName IS NULL
       |"""
      .stripMargin

  def differences(
    spark: SparkSession,
    log: Logger[IO],
    dfPrevious: DataFrame,
    dfLatest: DataFrame,
    pkName: String
  ): IO[DataFrame] =
    for {
      _ <- log.info("Schema for previous:" + dfPrevious.schema.fields.toList.toString())
        _ <- log.info("Schema for latest:  " + dfLatest.schema.fields.toList.toString())
        columnNames <- columnNames(dfPrevious) // either dfPrevious or dfLatest will do
        sqlString = differencesSql(nameLatest, namePrevious, pkName, columnNames)
        _ <- log.info(s"differencesSql [$sqlString]")
        df <- SparkApp.executeSql(spark, sqlString)
    } yield df

  def differencesSql(lhsName: String, rhsName: String, pkName: String, columnNames: List[String]): String =
    s"""
       |SELECT l.*
       |FROM $lhsName l
       |INNER JOIN $rhsName r
       |  ON r.$pkName = l.$pkName
       |WHERE NOT (${columnNames.map(c => s"l.$c = r.$c").mkString(" AND ")})
       |"""
      .stripMargin

  def sendUpsertsToKafka(df: DataFrame, log: Logger[IO]): IO[Unit] =
    IO.delay {
      df.collect()
        .foreach(r => log.info(s"Upsert $r")) // TODO
    }

  def sendDeletesToKafka(df: DataFrame, log: Logger[IO]): IO[Unit] =
    IO.delay {
      df.collect()
        .foreach(r => log.info(s"Delete $r")) // TODO
    }

  def removeDirs(history: List[String]): IO[Unit] =
    history.traverse_ {
      path =>
        for {
          table <- IO.delay(DeltaTable.forPath(path))
            _ <- IO.delay(table.delete())
            _ <- IO.delay(table.vacuum().collect())
        } yield ()
    }

  // Write the latest snapshot
  def writeLatestSnapshot(dfLatest: DataFrame, log: Logger[IO], pkName: String, timestamp: OffsetDateTime): IO[Unit] =
    for {
      path <- topicDataPath(Gold, s"samplequery_$timestamp").pure[IO]
        _ <- log.info(s"Writing first snapshot $path")
        _ <-
          IO.delay {
            dfLatest.write
              .partitionBy(pkName)
              .format("delta")
              .save(path)
          }
    } yield ()

  def columnNames(df: DataFrame): IO[List[String]] =
    IO.delay(df.schema.fields.map(_.name).toList)

  def nameDataframe(df: DataFrame, log: Logger[IO], name: String): IO[Unit] =
    log.info(s"Established name $name") >> // TODO move logging to callers
      IO.delay(df.createOrReplaceTempView(name))
}
