/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerObjectInteractions}
import akka.util.ByteString

class SpannerObjectStoreSpec extends SpannerSpec("SpannerObjectStoreSpec") {
  override def withObjectStore: Boolean = true

  val grpcClient = SpannerGrpcClientExtension(classicSystem.toTyped).clientFor("akka.persistence.spanner")
  val spannerInteractions = new SpannerObjectInteractions(
    grpcClient,
    spannerSettings
  )
  val serId = 5749231L

  "The spanner object store" should {
    "save and retrieve a value" in {
      val key = "my-key"
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(key, serId, "manifest-type-information", value).futureValue
      spannerInteractions.getObject(key).futureValue should be(value)
    }
    "produce an error when fetching a non-existing key" in {
      spannerInteractions.getObject("nonexistent-key").failed.futureValue.getMessage should include(
        "No data found for key [nonexistent-key]"
      )
    }
    "update a value" in {
      val key = "key-to-be-updated"
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(key, serId, "manifest-type-information", value).futureValue
      spannerInteractions.getObject(key).futureValue should be(value)

      val updatedValue = ByteString("Open to Feedback")
      spannerInteractions.upsertObject(key, serId, "manifest-type-information", updatedValue).futureValue
      spannerInteractions.getObject(key).futureValue should be(updatedValue)
    }
    "support deletions" in {
      val key = "to-be-added-and-removed"
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(key, serId, "manifest-type-information", value).futureValue
      spannerInteractions.getObject(key).futureValue should be(value)
      spannerInteractions.deleteObject(key).futureValue
      spannerInteractions.getObject(key).failed.futureValue.getMessage should include(
        s"No data found for key [$key]"
      )
    }
  }
}
