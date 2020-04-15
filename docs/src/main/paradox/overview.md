# Overview

The Akka Persistence Spanner plugin 

## Project Info

@@project-info{ projectId="journal" }

## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-spanner_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka 2.6.x and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

@@dependencies{ projectId="journal" }

## Contributing

Please feel free to contribute to Akka and Akka Persistence Spanner Documentation by reporting issues you identify, or by suggesting changes to the code. 
Please refer to our [contributing instructions](https://github.com/akka/akka/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://www.lightbend.com/conduct).
