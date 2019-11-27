package poc.spark.deltas.shared

import cats.Applicative
import cats.effect.Sync
import cats.syntax.applicative._
import cats.syntax.either._
import cats.syntax.functor._

trait Logger[F[_]] {
  def error(message: => String): F[Unit]
  def warn(message: => String): F[Unit]
  def info(message: => String): F[Unit]
  def debug(message: => String): F[Unit]
}

object Logger {
  def slf4j[F[_] : Applicative, G[_] : Sync](prefix: String): G[Logger[F]] =
    Sync[G].delay(org.slf4j.LoggerFactory.getLogger(getClass))
      .map(slf4jImpl[F](_, if (prefix.isEmpty) "" else s"#### $prefix: "))

  def console[F[_] : Applicative, G[_] : Sync]: G[Logger[F]] =
    Sync[G].delay(consoleImpl[F])

  def silent[F[_] : Applicative, G[_] : Sync]: G[Logger[F]] =
    Sync[G].delay(silentImpl[F])

  ////

  private def slf4jImpl[F[_] : Applicative](slf: org.slf4j.Logger, prefix: String): Logger[F] =
    new Logger[F] {
      override def error(message: => String): F[Unit] =
        safely(slf.error(s"$prefix$message"), message)

      override def warn(message: => String): F[Unit] =
        safely(slf.warn(s"$prefix$message"), message)

      override def info(message: => String): F[Unit] =
        safely(slf.info(s"$prefix$message"), message)

      override def debug(message: => String): F[Unit] =
        safely(slf.debug(s"$prefix$message"), message)

      def safely(op: => Unit, message: String): F[Unit] =
        Either.catchNonFatal(op)
          .pure[F]
          .map(_.fold(e => println(s"PANIC: $e\nAttempted message: $message"), identity))
    }

  private def consoleImpl[F[_] : Applicative]: Logger[F] =
    new Logger[F] {
      override def error(message: => String): F[Unit] =
        println(message).pure

      override def warn(message: => String): F[Unit] =
        println(message).pure

      override def info(message: => String): F[Unit] =
        println(message).pure

      override def debug(message: => String): F[Unit] =
        println(message).pure
    }

  private def silentImpl[F[_] : Applicative]: Logger[F] =
    new Logger[F] {
      override def error(message: => String): F[Unit] =
        ().pure

      override def warn(message: => String): F[Unit] =
        ().pure

      override def info(message: => String): F[Unit] =
        ().pure

      override def debug(message: => String): F[Unit] =
        ().pure
    }
}
