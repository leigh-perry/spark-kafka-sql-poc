package poc.spark.deltas.app.service

import poc.spark.deltas.shared.Log
import org.apache.spark.sql.SparkSession
import poc.spark.deltas.shared.Log
import zio.ZIO
import zio.blocking.Blocking

final case class AppConfig(kafkaBrokers: String, schemaRegistryAddr: String)

trait AppConfigError
final case class ConfigEnvError(exception: Throwable) extends AppConfigError

////

/**
 * Support for loading stuff the app needs in order to get started
 */
object Bootstrap {
  trait ConfigSupport {
    def load: ZIO[Any, AppConfigError, AppConfig]
  }

  object ConfigSupport {
    trait Live extends ConfigSupport {
      override def load: ZIO[Any, AppConfigError, AppConfig] =
        ZIO.succeed(AppConfig("TODO", "TODO"))
    }

    object Live extends Live
  }

  object TestConfiguration {
    trait Test extends ConfigSupport {
      override def load: ZIO[Any, AppConfigError, AppConfig] =
        ZIO.succeed(
          AppConfig(
            kafkaBrokers = "localhost:9092",
            schemaRegistryAddr = "http://localhost:8081"
          )
        )
    }

    object Test extends Test
  }

  ////

  trait LogSupport {
    def load(prefix: String): ZIO[Any, Nothing, Log]
  }

  object LogSupport {
    trait Live extends LogSupport {
      override def load(prefix: String): ZIO[Any, Nothing, Log] =
        Log.slf4j(prefix)
    }

    object Live extends Live
  }

  object TestLogSupport {
    trait Test extends LogSupport {
      override def load(prefix: String): ZIO[Any, Nothing, Log] =
        Log.slf4j(prefix)
    }

    object Test extends Test
  }

  ////

  trait SparkSupport {
    def sparkSession(name: String): ZIO[Blocking, Throwable, SparkSession]
  }

  object SparkSupport {
    trait Live extends SparkSupport {
      override def sparkSession(name: String): ZIO[Blocking, Throwable, SparkSession] =
        ZIO.accessM {
          _.blocking
            .effectBlocking(
              SparkSession
                .builder()
                .appName(name)
                .enableHiveSupport()
                .getOrCreate()
            )
        }
    }

    object Live extends Live
  }

  object TestSparkSupport {
    trait Test extends SparkSupport {
      override def sparkSession(name: String): ZIO[Blocking, Throwable, SparkSession] =
        zio
          .blocking
          .effectBlocking(
            SparkSession
              .builder()
              .appName(name)
              .master("local")
              .getOrCreate()
          )
    }

    object Test extends Test
  }
}

////

/**
 * Stuff loaded in the bootstrap phase and is now available to the app while it is running
 */
object AppRuntime {
  trait Config {
    val cfg: AppConfig
  }

  trait Logging {
    val log: Log
  }

  trait Spark {
    val sparkSession: SparkSession
  }

  case class All(
    cfg: AppConfig,
    log: Log,
    sparkSession: SparkSession
  ) extends Config
    with Logging
    with Spark
}
