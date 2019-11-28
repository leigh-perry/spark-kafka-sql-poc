resolvers += "simplemachines releases" at "https://nexus.simplemachines.com.au/content/repositories/public-releases"
resolvers += Resolver.url("sbts3 ivy resolver", url("https://dl.bintray.com/emersonloureiro/sbt-plugins"))(
  Resolver.ivyStylePatterns
)

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.5.0")
addSbtPlugin("com.cavorite" % "sbt-avro-1-8" % "1.1.5")
addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.0.0-RC15")

lazy val root = project
  .in(file("."))
  .settings(
    // Workaroud for the sbt s3 plugin AWS SDK version not supporting Java 11
    // Ref: https://github.com/aws/aws-xray-sdk-java/issues/38
    // Ref: https://stackoverflow.com/questions/43574426/how-to-resolve-java-lang-noclassdeffounderror-javax-xml-bind-jaxbexception-in-j
    libraryDependencies += "javax.xml.bind" % "jaxb-api" % "2.3.0"
  )
