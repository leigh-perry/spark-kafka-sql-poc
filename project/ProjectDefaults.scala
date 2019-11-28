import Dependencies._
import sbt._
import Keys._

import sbt.Package.ManifestAttributes

object ProjectDefaults extends AutoPlugin {

  // Set plugin to autoload
  override def trigger = allRequirements

  lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

  override lazy val projectSettings = {
    val scalacWarnings =
      "-deprecation" ::
        "-feature" ::
        "-unchecked" ::
        "-Ywarn-dead-code" ::
        "-Ywarn-inaccessible" ::
        "-Ywarn-infer-any" ::
        "-Ywarn-nullary-override" ::
        "-Ywarn-nullary-unit" ::
        "-Ywarn-numeric-widen" ::
        "-Ywarn-value-discard" ::
        "-Xlint:_,-type-parameter-shadow" ::
        Nil

    Seq(
      scalacOptions ++= Seq(
        "-encoding",
        "UTF-8",
        "-explaintypes",
        "-Yrangepos",
        "-Xfuture",
        "-Ypartial-unification",
        "-language:higherKinds",
        "-language:existentials",
        "-Yno-adapted-args",
        "-Xsource:2.13",
        "-target:jvm-1.8"
      ) ++ scalacWarnings,
      // Disable warnings in the console as it becomes unusable
      scalacOptions in (Compile, console) ~= { _ filterNot scalacWarnings.contains },
      scalacOptions in (Test, console) ~= { _ filterNot scalacWarnings.contains },
      javacOptions ++= {
        //Hacky Hack Hack, work around for needing Java 1,8, running multiple JVMS etc etc
        System.getProperty("java.version").split('.').toList match {
          case "1" :: "8" :: _ => Seq("-target", "1.8", "-source", "1.8")
          case _ => Seq("--release", "8") // JVM 8+
        }
      },
      organization := "com.leighperry",
      updateOptions := updateOptions.value.withCachedResolution(true),
      resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
      parallelExecution in Test := false,
      credentials ++= {
        val envSrc: Seq[File] = sys.env.get("IVY_CREDENTIALS_FILE").map(Path.apply(_).asFile).toSeq
        val fileSrc: Seq[File] = (Path.userHome / ".ivy2" / ".credentials").filter(_.exists).get
        (envSrc ++ fileSrc).map(Credentials.apply)
      },
      // Force Maven publishing to be opt in
      skip in publish := true,
      // Prevent java / scala-212 directories being created
      Compile / unmanagedSourceDirectories ~= { _.filter(_.exists) },
      Test / unmanagedSourceDirectories ~= { _.filter(_.exists) },
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.10"),
      // Add specs2 core test lib by default to every project
      libraryDependencies ++= Seq(specs2Core, specs2Matchers, specs2Scalacheck).map(_ % "test"),

      packageOptions in (Compile, packageBin) ~= {
        _.foldLeft(List[PackageOption](ManifestAttributes(("Multi-Release", "true")))) {
          case (result, ManifestAttributes(attributes @ _*)) =>
            ManifestAttributes(attributes.filterNot(_._1.toString.toLowerCase.contains("version")): _*) :: result
          case (result, x) =>
            x :: result
        }
      }
    )

  }

}
