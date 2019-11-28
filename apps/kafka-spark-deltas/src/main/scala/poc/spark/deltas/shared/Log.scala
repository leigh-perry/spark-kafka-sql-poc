package poc.spark.deltas.shared

import zio.ZIO
import zio.console.Console

trait Log {
  def error(message: => String): ZIO[Any, Nothing, Unit]
  def warn(message: => String): ZIO[Any, Nothing, Unit]
  def info(message: => String): ZIO[Any, Nothing, Unit]
  def debug(message: => String): ZIO[Any, Nothing, Unit]
}

object Log {
  def slf4j(prefix: String): ZIO[Any, Nothing, Log] =
    ZIO
      .effect(org.slf4j.LoggerFactory.getLogger(getClass))
      .map(slf4jImpl(_, if (prefix.isEmpty) prefix else s"$prefix: "))
      .catchAll {
        e =>
          Console.Live.console.putStrLn(s"PANIC: $e\nReverted to console logging")
          console
      }

  def console: ZIO[Any, Nothing, Log] =
    ZIO.effectTotal(consoleImpl)

  def silent: ZIO[Any, Nothing, Log] =
    ZIO.effectTotal(silentImpl)

  ////

  private def slf4jImpl(slf: org.slf4j.Logger, prefix: String): Log =
    new Log {
      override def error(message: => String): ZIO[Any, Nothing, Unit] =
        safely(slf.error(s"$prefix$message"), message)

      override def warn(message: => String): ZIO[Any, Nothing, Unit] =
        safely(slf.warn(s"$prefix$message"), message)

      override def info(message: => String): ZIO[Any, Nothing, Unit] =
        safely(slf.info(s"$prefix$message"), message)

      override def debug(message: => String): ZIO[Any, Nothing, Unit] =
        safely(slf.debug(s"$prefix$message"), message)

      def safely(op: => Unit, message: String): ZIO[Any, Nothing, Unit] =
        ZIO
          .effect(op)
          .either
          .map(
            _.fold(
              e => Console.Live.console.putStrLn(s"PANIC: $e\nAttempted message: $message"),
              identity
            )
          )
          .unit
    }

  private def consoleImpl: Log =
    new Log {
      override def error(message: => String): ZIO[Any, Nothing, Unit] =
        Console.Live.console.putStrLn(message)

      override def warn(message: => String): ZIO[Any, Nothing, Unit] =
        Console.Live.console.putStrLn(message)

      override def info(message: => String): ZIO[Any, Nothing, Unit] =
        Console.Live.console.putStrLn(message)

      override def debug(message: => String): ZIO[Any, Nothing, Unit] =
        Console.Live.console.putStrLn(message)
    }

  private def silentImpl: Log =
    new Log {
      override def error(message: => String): ZIO[Any, Nothing, Unit] =
        ZIO.effectTotal(())

      override def warn(message: => String): ZIO[Any, Nothing, Unit] =
        ZIO.effectTotal(())

      override def info(message: => String): ZIO[Any, Nothing, Unit] =
        ZIO.effectTotal(())

      override def debug(message: => String): ZIO[Any, Nothing, Unit] =
        ZIO.effectTotal(())
    }
}
