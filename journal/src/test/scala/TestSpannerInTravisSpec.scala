import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import com.google.longrunning.Operation
import com.google.spanner.admin.database.v1.{CreateDatabaseRequest, DatabaseAdminClient}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

class TestSpannerInTravisSpec extends AnyWordSpec with ScalaFutures with Matchers {
  val project = "akka"
  val instance = "akka"
  val database = "TestSpannerInTravisSpec"

  val table =
    """
  CREATE TABLE messages (
        persistence_id STRING(MAX) NOT NULL,
        sequence_nr INT64 NOT NULL,
        payload BYTES(MAX),
        ser_id INT64 NOT NULL,
        ser_manifest STRING(MAX) NOT NULL,
        tags ARRAY<STRING(MAX)>,
        write_time TIMESTAMP OPTIONS (allow_commit_timestamp=true),
        writer_uuid STRING(MAX) NOT NULL,
) PRIMARY KEY (persistence_id, sequence_nr)
     """

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  "Can travis" should {
    "access spanner" in {
      implicit val system = ActorSystem()
      implicit val ec = system.dispatcher

      val settings = GrpcClientSettings.fromConfig("spanner-client")
      val parent = s"projects/$project/instances/$instance"
      val fullyQualifiedDbName: String = s"$parent/databases/$database"

      val adminClient = DatabaseAdminClient(settings)
      val done: Operation =
        adminClient.createDatabase(CreateDatabaseRequest(parent = parent, s"CREATE DATABASE $database")).futureValue
      done.done shouldEqual true
    }
  }
}
