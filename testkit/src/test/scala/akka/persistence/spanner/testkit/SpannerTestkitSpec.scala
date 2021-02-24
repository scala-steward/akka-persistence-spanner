/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.testkit

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpecLike

object SpannerTestkitSpec {
  val config = ConfigFactory.parseString("""
      #emulator-config                                       
      akka.persistence.journal.plugin = "akka.persistence.spanner.journal"
      akka.persistence.spanner {
        session-pool {
          max-size = 1
        }
        use-auth = false
      }
      akka.grpc.client.spanner-client {
        host = localhost
        port = 9010
        use-tls = false
      }
      #emulator-config                                       
    """)

  case class Pong()
  case class Ping(replyTo: ActorRef[Pong])
  val pingPong: Behavior[Ping] = EventSourcedBehavior[Ping, String, String](
    PersistenceId.ofUniqueId("pid-1"),
    "",
    (_, ping) => Effect.persist("pinged").thenRun(_ => ping.replyTo ! Pong()),
    (_, _) => ""
  )
}

class SpannerTestkitSpec
    extends ScalaTestWithActorTestKit(SpannerTestkitSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfter {
  import SpannerTestkitSpec._

  //#setup
  val spannerTestkit = new SpannerTestkit(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spannerTestkit.createDatabaseAndSchema()
  }

  protected override def afterAll(): Unit = {
    spannerTestkit.dropDatabase()
    super.afterAll()
  }
  //#setup

  "SpannerTestkit" should {
    "have created the database" in {
      val probe = createTestProbe[Pong]()
      val p1 = spawn(pingPong)
      p1 ! Ping(probe.ref)
      probe.expectMessage(Pong())
    }
  }
}
