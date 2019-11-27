resolvers += "simplemachines releases" at "https://nexus.simplemachines.com.au/content/repositories/public-releases"

addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.0.0-RC15")
addSbtPlugin("au.com.simplemachines" % "sbt-kafka-compose"      % "0.2.3")
addSbtPlugin("com.cavorite"          % "sbt-avro-1-8"           % "1.1.9")
