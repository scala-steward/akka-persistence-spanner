import sbt.Keys.parallelExecution
import xerial.sbt.Sonatype.autoImport.sonatypeProfileName

inThisBuild(
  Seq(
    organization := "com.lightbend.akka",
    organizationName := "Lightbend Inc.",
    homepage := Some(url("https://doc.akka.io/docs/akka-persistence-spanner/current")),
    scmInfo := Some(
        ScmInfo(
          url("https://github.com/akka/akka-persistence-spanner"),
          "https://github.com/akka/akka-persistence-spanner.git"
        )
      ),
    startYear := Some(2020),
    developers += Developer(
        "contributors",
        "Contributors",
        "https://gitter.im/akka/dev",
        url("https://github.com/akka/akka-persistence-spanner/graphs/contributors")
      ),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    description := "A replicated Akka Persistence journal backed by Spanner",
    // due to the emulator
    parallelExecution := false,
    // add snapshot repo when Akka version overriden
    resolvers ++=
      (if (System.getProperty("override.akka.version") != null)
         Seq("Akka Snapshots".at("https://repo.akka.io/snapshots/"))
       else Seq.empty)
  )
)

def common: Seq[Setting[_]] = Seq(
  crossScalaVersions := Seq(Dependencies.Scala213, Dependencies.Scala212),
  scalaVersion := Dependencies.Scala212,
  crossVersion := CrossVersion.binary,
  scalafmtOnCompile := true,
  sonatypeProfileName := "com.lightbend",
  // Setting javac options in common allows IntelliJ IDEA to import them automatically
  javacOptions in compile ++= Seq(
      "-encoding",
      "UTF-8",
      "-source",
      "1.8",
      "-target",
      "1.8"
    ),
  headerLicense := Some(
      HeaderLicense.Custom(
        """Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>"""
      )
    ),
  logBuffered in Test := System.getProperty("akka.logBufferedTests", "false").toBoolean,
  // show full stack traces and test case durations
  testOptions in Test += Tests.Argument("-oDF"),
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -a Show stack traces and exception class name for AssertionErrors.
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value)
)

lazy val dontPublish = Seq(
  skip in publish := true,
  whitesourceIgnore := true,
  publishArtifact in Compile := false
)

lazy val root = (project in file("."))
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "akka-persistence-spanner-root",
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
  )
  .aggregate(journal, testkit)

lazy val dumpSchema = taskKey[Unit]("Dumps schema for docs")
dumpSchema := (journal / runMain in (Test)).toTask(" akka.persistence.spanner.PrintSchema").value

def suffixFileFilter(suffix: String): FileFilter = new SimpleFileFilter(f => f.getAbsolutePath.endsWith(suffix))

lazy val journal = (project in file("journal"))
  .enablePlugins(AkkaGrpcPlugin)
  .settings(common)
  .settings(
    name := "akka-persistence-spanner",
    libraryDependencies ++= Dependencies.journal,
    // Workaround for https://github.com/akka/akka-persistence-spanner/issues/62
    excludeFilter in PB.generate ~= (
          f =>
            f ||
            suffixFileFilter("google/protobuf/any.proto") ||
            suffixFileFilter("google/protobuf/api.proto") ||
            suffixFileFilter("google/protobuf/descriptor.proto") ||
            suffixFileFilter("google/protobuf/duration.proto") ||
            suffixFileFilter("google/protobuf/empty.proto") ||
            suffixFileFilter("google/protobuf/field_mask.proto") ||
            suffixFileFilter("google/protobuf/source_context.proto") ||
            suffixFileFilter("google/protobuf/struct.proto") ||
            suffixFileFilter("google/protobuf/timestamp.proto") ||
            suffixFileFilter("google/protobuf/type.proto") ||
            suffixFileFilter("google/protobuf/wrappers.proto")
        )
  )

lazy val testkit = (project in file("testkit"))
  .settings(common)
  .dependsOn(journal)
  .settings(
    name := "akka-persistence-spanner-testkit",
    libraryDependencies ++= Dependencies.testkit
  )

lazy val example = (project in file("example"))
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "akka-persistence-spanner-example",
    libraryDependencies ++= Dependencies.example,
    fork in run := false,
    Global / cancelable := false // let ctrl-c kill sbt
  )
  .dependsOn(journal)

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PublishRsyncPlugin)
  .dependsOn(journal)
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "Akka Persistence Spanner",
    (Compile / paradox) := (Compile / paradox).dependsOn(root / dumpSchema).value,
    crossScalaVersions := Seq(Dependencies.Scala212),
    previewPath := (Paradox / siteSubdirName).value,
    Paradox / siteSubdirName := s"docs/akka-persistence-spanner/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    Compile / paradoxProperties ++= Map(
        "project.url" -> "https://doc.akka.io/docs/akka-persistence-spanner/current/",
        "canonical.base_url" -> "https://doc.akka.io/docs/akka-persistence-spanner/current",
        "akka.version" -> Dependencies.AkkaVersion,
        "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
        "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
        "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
        "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/"
      ),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifact := makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io"
  )
