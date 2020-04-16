package akka.persistence.spanner

import com.typesafe.config.Config

final class SpannerSettings(config: Config) {
  val project = config.getString("project")
  val instance = config.getString("instance")
  val database = config.getString("database")

  val fullyQualifiedProject = s"projects/$project"
  val parent = s"$fullyQualifiedProject/instances/$instance"
  val fullyQualifiedDatabase = s"$parent/databases/$database"
}
