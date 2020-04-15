/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {
  val AkkaVersion = "2.6.4"

  val SpannerVersion = "1.52.0"
  // Keep in sync with Akka gRPC
  val GrpcVersion = "1.28.0"
  val GoogleAuthVersion = "0.20.0"


  object Compile {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion

    val spannerProtobuf = "com.google.api.grpc" % "proto-google-cloud-spanner-v1" % SpannerVersion % "protobuf-src"
    val grpcAuth = "io.grpc" % "grpc-auth" % GrpcVersion
    val googleAuth = "com.google.auth" % "google-auth-library-oauth2-http" % GoogleAuthVersion

  }

  object TestDeps {
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test
    val akkaTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test

    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" % Test // EPL 1.0 / LGPL 2.1
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8" % Test // ApacheV2
    val junit = "junit" % "junit" % "4.12" % Test
    val junitInterface = "com.novocode" % "junit-interface" % "0.11" % Test

  }

  import Compile._
  import TestDeps._

  val journal = Seq(
    akkaPersistence,
    akkaPersistenceQuery,
    akkaPersistenceTck,
    akkaStreamTestkit,
    spannerProtobuf,
    grpcAuth,
    googleAuth,
    akkaTestkit,
    logback,
    scalaTest
  )

}
