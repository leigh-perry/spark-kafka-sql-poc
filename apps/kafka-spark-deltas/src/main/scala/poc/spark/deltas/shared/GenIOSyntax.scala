package poc.spark.deltas.shared

import zio.ZIO

import scala.language.implicitConversions

final class GenIOSyntaxSafeOps[R, E](op: => Unit) {
  def safely: ZIO[R, Nothing, Unit] =
    ZIO(op)
      .catchAll[R, Nothing, Unit](e => ZIO.effectTotal(println(s"PANIC: $e")))
}

trait ToGenIOSyntaxSafeOps {
  implicit def implToGenIOSyntaxSafeOps[R, E](op: => Unit): GenIOSyntaxSafeOps[R, E] =
    new GenIOSyntaxSafeOps[R, E](op)
}

////

final class GenIOSyntaxSafeOpsIO[R, E](io: ZIO[R, E, Unit]) {
  def safely: ZIO[R, Nothing, Unit] =
    io.catchAll(e => ZIO.effectTotal(println(s"PANIC: $e")))
}

trait ToGenIOSyntaxSafeOpsIO {
  implicit def implToGenIOSyntaxSafeOpsIO[R, E](io: ZIO[R, E, Unit]): GenIOSyntaxSafeOpsIO[R, E] =
    new GenIOSyntaxSafeOpsIO[R, E](io)
}

////

trait GenIOSyntax extends ToGenIOSyntaxSafeOps with ToGenIOSyntaxSafeOpsIO

object geniosyntaxinstances extends GenIOSyntax
