package poc.spark.deltas.app

import java.io.File
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import poc.spark.deltas.app.service.{AppConfigError, AppRuntime, Bootstrap}
import poc.spark.deltas.shared.{Deltalake, DeltalakeLevel, Log}
import org.apache.spark.sql.DataFrame
import poc.spark.deltas.app.service.{AppConfigError, AppRuntime}
import poc.spark.deltas.app.service.Bootstrap.ConfigSupport.Live
import poc.spark.deltas.app.service.Bootstrap.SparkSupport
import poc.spark.deltas.shared.DeltalakeLevel.Gold
import poc.spark.deltas.shared.{Deltalake, DeltalakeLevel, Log}
import zio.blocking.Blocking
import zio.{App, ZIO}

object GoldSqlJoinJob extends App {
  sealed trait MainAppError
  final case class ConfigLoadError(message: AppConfigError) extends MainAppError
  final case class ExceptionEncountered(exception: Throwable) extends MainAppError
  final case class SparkSqlFailure(exception: Throwable) extends MainAppError

  override def run(args: List[String]): ZIO[Environment, Nothing, Int] =
    for {
      log <- Log.slf4j(prefix = "GOLD")
      r <- resolvedProgram(log)
        .foldM(
          e => log.error(s"Application failed: $e") *> ZIO.succeed(1),
          _ => log.info("Application terminated with no error indication") *> ZIO.succeed(0)
        )
    } yield r

  private def resolvedProgram(log: Log): ZIO[Blocking, MainAppError, Unit] =
    for {
      // Config is loaded separately in order to provide to `program`
      cfg <- Live.load.mapError(ConfigLoadError)

      sparkSession <- SparkSupport.Live
        .sparkSession("GOLD")
        .mapError(ExceptionEncountered)

      appRuntime = new AppRuntime.All(cfg, log, sparkSession) with Deltalake.Live {}

      // TODO make a program argument
      requestedSqlString = """
                             |SELECT x.userid, x.name, x.regionid, y.refcount, y.optional, x.gender
                             |FROM xx x
                             |INNER JOIN yy y
                             |  ON x.userid = y.userid
                             |""".stripMargin

      resolved <- program(requestedSqlString, "userid").provide(appRuntime)
    } yield resolved

  ////

  // Local shortcuts
  implicit class TaskOps[R, A](io: ZIO[R, Throwable, A]) {
    def mapped: ZIO[R, MainAppError, A] =
      io.mapError(ExceptionEncountered)
  }

  val namePrevious = "previousDF"
  val nameLatest = "latestDF"

  def program(
    sqlString: String,
    pkName: String
  ): ZIO[
    AppRuntime.Config with AppRuntime.Logging with AppRuntime.Spark with Deltalake,
    MainAppError,
    Unit
  ] =
    ZIO.accessM {
      env =>
        for {
          // TODO generalise from parsed SQL (for topic/table names) etc
          timestamp <- ZIO.effect(Instant.now.atOffset(ZoneOffset.UTC)).mapped
          // TODO infer topics from SQL
          _ <- env.deltalake.setupBatchTopicTableDataframe(env.sparkSession, env.log, "xx").mapped
          _ <- env.deltalake.setupBatchTopicTableDataframe(env.sparkSession, env.log, "yy").mapped
          goldDir <- ZIO.effect(new File(env.deltalake.basePath(Gold))).mapped
          snapshotNames = if (goldDir.exists) goldDir.listFiles.sorted.reverse.map(_.getName).toList
          else List.empty
          _ <- env.log.info(s"snapshotNames: $snapshotNames ")
          _ <- emitResultChanges(snapshotNames, pkName, timestamp, sqlString)
        } yield ()
    }

  def emitResultChanges(
    snapshotNames: List[String],
    pkName: String,
    timestamp: OffsetDateTime,
    sqlString: String
  ): ZIO[AppRuntime.Logging with AppRuntime.Spark with Deltalake, MainAppError, Unit] =
    ZIO.accessM {
      env =>
        snapshotNames match {
          // First execution - all inserts
          case Nil =>
            for {
              _ <- env.log.info(s"First execution - no prior snapshots")
              dfLatest <- evaluateLatest(sqlString)
              _ <- sendUpsertsToKafka(dfLatest)
              _ <- writeLatestSnapshot(dfLatest, pkName, timestamp)
            } yield ()

          // Subsequent executions - work out deltas as inserts, updates, deletes
          case prior :: history =>
            for {
              _ <- env.log.info(s"Most recent snapshot is $prior")
              dfLatest <- evaluateLatest(sqlString)
              dfPrevious <- loadSnapshot(prior)
              inserts <- leftOnly(nameLatest, dfLatest, namePrevious, dfPrevious, pkName)
              updates <- differences(dfPrevious, dfLatest, pkName)
              deletes <- leftOnly(namePrevious, dfPrevious, nameLatest, dfLatest, pkName)
              _ <- ZIO.sequencePar(
                List(
                  sendUpsertsToKafka(inserts),
                  sendUpsertsToKafka(updates),
                  sendDeletesToKafka(deletes)
                )
              )
              // TODO remove partial snapshot if any failure... or use success flag??? bracket???
              _ <- writeLatestSnapshot(dfLatest, pkName, timestamp)
              _ <- removeSnapshots(history)
            } yield ()
        }
    }

  def evaluateLatest(
    sqlString: String
  ): ZIO[AppRuntime.Logging with AppRuntime.Spark, MainAppError, DataFrame] =
    ZIO.accessM {
      env =>
        for {
          sqlString <- ZIO.effect(sqlString).mapped
          _ <- env.log.info(s"latest [$sqlString]    ")
          df <- executeSql(sqlString)
          _ <- nameDataframe(df, nameLatest)
          _ <- env.log.info("Evaluated latest snapshot")
        } yield df
    }

  def loadSnapshot(
    snapshotName: String
  ): ZIO[AppRuntime.Logging with AppRuntime.Spark with Deltalake, MainAppError, DataFrame] =
    ZIO.accessM {
      env =>
        for {
          path <- ZIO.succeed(env.deltalake.topicDataPath(DeltalakeLevel.Gold, snapshotName))
          df <- env.deltalake.batchDataframe(env.sparkSession, env.log, path).mapped
          _ <- nameDataframe(df, namePrevious)
          _ <- env.log.info(s"Loaded dataframe for $path ")
        } yield df
    }

  def leftOnly(
    lhsName: String,
    lhs: DataFrame,
    rhsName: String,
    rhs: DataFrame,
    pkName: String
  ): ZIO[AppRuntime.Logging with AppRuntime.Spark with Deltalake, MainAppError, DataFrame] =
    ZIO.accessM {
      env =>
        for {
          _ <- env.log.info("Schema for lhs:" + lhs.schema.fields.toList.toString())
          _ <- env.log.info("Schema for rhs:" + rhs.schema.fields.toList.toString())
          columnNames <- columnNames(lhs) // either lhs or rhs will do
          sqlString = leftOnlySql(lhsName, rhsName, pkName, columnNames)
          _ <- env.log.info(s"leftOnlySql [$sqlString]")
          df <- executeSql(sqlString)
        } yield df
    }

  def leftOnlySql(
    lhsName: String,
    rhsName: String,
    pkName: String,
    columnNames: List[String]
  ): String =
    s"""
       |SELECT l.*
       |FROM $lhsName l
       |LEFT OUTER JOIN $rhsName r
       |  ON l.$pkName = r.$pkName
       |WHERE r.$pkName IS NULL
       |""".stripMargin

  def differences(
    dfPrevious: DataFrame,
    dfLatest: DataFrame,
    pkName: String
  ): ZIO[AppRuntime.Logging with AppRuntime.Spark with Deltalake, MainAppError, DataFrame] =
    ZIO.accessM {
      env =>
        for {
          _ <- env.log.info("Schema for previous:" + dfPrevious.schema.fields.toList.toString())
          _ <- env.log.info("Schema for latest:  " + dfLatest.schema.fields.toList.toString())
          columnNames <- columnNames(dfPrevious) // either dfPrevious or dfLatest will do
          sqlString = differencesSql(nameLatest, namePrevious, pkName, columnNames)
          _ <- env.log.info(s"differencesSql [$sqlString]")
          df <- executeSql(sqlString)
        } yield df
    }

  def differencesSql(
    lhsName: String,
    rhsName: String,
    pkName: String,
    columnNames: List[String]
  ): String =
    s"""
       |SELECT l.*
       |FROM $lhsName l
       |INNER JOIN $rhsName r
       |  ON r.$pkName = l.$pkName
       |WHERE NOT (${columnNames.map(c => s"l.$c = r.$c").mkString(" AND ")})
       |""".stripMargin

  def sendUpsertsToKafka(df: DataFrame): ZIO[AppRuntime.Logging, MainAppError, Unit] =
    ZIO.accessM {
      env =>
        for {
          r <- ZIO.effect {
            df.collect()
              .foreach(r => env.log.info(s"Upsert $r")) // TODO implement
          }.mapped
        } yield r
    }

  def sendDeletesToKafka(df: DataFrame): ZIO[AppRuntime.Logging, MainAppError, Unit] =
    ZIO.accessM {
      env =>
        ZIO.effect {
          df.collect()
            .foreach(r => env.log.info(s"Delete $r")) // TODO implement
        }.mapped
    }

  def removeSnapshots(history: List[String]): ZIO[Deltalake, MainAppError, Unit] =
    ZIO.accessM {
      env =>
        ZIO.traverse_(history) {
          env
            .deltalake
            .removeAt(_)
            .mapped
        }
    }

  // Write the latest snapshot
  def writeLatestSnapshot(
    dfLatest: DataFrame,
    pkName: String,
    timestamp: OffsetDateTime
  ): ZIO[AppRuntime.Logging with Deltalake, MainAppError, Unit] =
    ZIO.accessM {
      env =>
        for {
          path <- ZIO.succeed(
            env.deltalake.topicDataPath(DeltalakeLevel.Gold, s"samplequery_$timestamp")
          )
          _ <- env.log.info(s"Writing first snapshot $path")
          _ <- ZIO.effect(dfLatest.write.partitionBy(pkName).format("delta").save(path)).mapped
        } yield ()
    }

  def columnNames(df: DataFrame): ZIO[Any, MainAppError, List[String]] =
    ZIO.effect(df.schema.fields.map(_.name).toList).mapped

  def nameDataframe(df: DataFrame, name: String): ZIO[AppRuntime.Logging, MainAppError, Unit] =
    ZIO.accessM {
      env =>
        for {
          _ <- env.log.info(s"Established name $name")
          _ <- ZIO.effect(df.createOrReplaceTempView(name)).mapped
        } yield ()
    }

  def executeSql(sqlString: String): ZIO[AppRuntime.Spark, MainAppError, DataFrame] =
    ZIO.accessM {
      env =>
        ZIO
          .effect(env.sparkSession.sql(sqlString))
          .mapError(SparkSqlFailure)
    }
}
