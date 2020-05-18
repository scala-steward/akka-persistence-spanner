# Getting Started

## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-spanner_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka 2.6.x and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

The plugin is published for Scala 2.13 and 2.12.

## Enabling

See configuring as a @ref:[journal](journal.md), @ref:[snapshot store](snapshots.md) or how to use the @ref:[query plugin](query.md)

## Local testing

The [Spanner emulator](https://cloud.google.com/spanner/docs/emulator) can be used to test without requiring connection to Spanner.

The emulator can be setup using the instructions in the Google documentation, using `gcloud`, or via Docker. Here's a sample docker compose
file that will start the emulator and forward the HTTP and gRPC ports of the emulator. This plugin only relies on gRPC.

@@snip [docker-compose.yml](/docker/docker-compose.yml)

The emulator only supports one concurrent transaction. This can be a major limitation for realistic testing. The emulator is best used for
basic verification and then realistic testing done with spanner. 

To run tests with the emulator you need the following configuration:

@@snip [spanner-emulator-config](/testkit/src/test/scala/akka/persistence/spanner/testkit/SpannerTestkitSpec.scala) { #emulator-config }

### Creating the schema

You can use the testkit to create and drop the database. Add test testkit dependency:

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-spanner-testkit_$scala.binary.version$
  version=$project.version$
}

To create and drop the database:

Java
:  @@snip [junit](/testkit/src/test/java/akka/persistence/spanner/testkit/SpannerTestkitTest.java) { #setup }

Scala
:  @@snip [scalatest](/testkit/src/test/scala/akka/persistence/spanner/testkit/SpannerTestkitSpec.scala) { #setup }


