package poc.spark.deltas.shared

import java.io.{PrintWriter, StringWriter}

import cats.effect.{ExitCode, IO}
import cats.syntax.apply._

object App {

  def runProgram(pgm: Logger[IO] => IO[Unit], logPrefix: String): IO[ExitCode] =
    for {
      log <- Logger.slf4j[IO, IO](logPrefix)
        outcome <- pgm(log).attempt
        code <- outcome.fold(
          e => log.error(s"Application failed: ${stackTrace(e)}") *> IO(ExitCode.Error),
          _ => log.info("Application terminated with no error indication") *> IO(ExitCode.Success)
        )
    } yield code

  def stackTrace(e: Throwable): String = {
    val sw = new StringWriter
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
      .trim
      .replaceAll("\tat ", "    <- ")
      .replaceAll("\t", "    ")
  }

}
