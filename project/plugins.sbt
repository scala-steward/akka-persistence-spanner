addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.1.1")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.2.0") // for maintenance of copyright file header
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.2.1")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.4.4")

// for releasing
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.9")

//// docs
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.1")
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.39")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")
