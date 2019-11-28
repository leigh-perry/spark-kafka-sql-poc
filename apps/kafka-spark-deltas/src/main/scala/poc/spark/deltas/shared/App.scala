package poc.spark.deltas.shared

import java.io.{ PrintWriter, StringWriter }

object App {
  def stackTrace(e: Throwable): String = {
    val sw = new StringWriter
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
      .trim
      .replaceAll("\tat ", "    <- ")
      .replaceAll("\t", "    ")
  }
}
