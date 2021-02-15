/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.spanner.internal.SpannerObjectInteractions.Result
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerObjectInteractions}
import akka.util.ByteString

class SpannerObjectStoreSpec extends SpannerSpec("SpannerObjectStoreSpec") {
  override def withObjectStore: Boolean = true

  val grpcClient = SpannerGrpcClientExtension(classicSystem.toTyped).clientFor("akka.persistence.spanner")
  val spannerInteractions = new SpannerObjectInteractions(
    grpcClient,
    spannerSettings
  )

  // All objects in this test use the same serialization ;)
  val serId = 5749231L
  val serManifest = "manifest-type-information"

  "The spanner object store" should {
    "save and retrieve a value" in {
      val key = "my-key"
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(key, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(key).futureValue should be(Result(value, serId, serManifest, 0L))
    }
    "save and retrieve a binary value" in {
      val key = "my-key"
      // this is not a valid UTF-8 string:
      val value = ByteString(Array[Byte](0xC0.toByte, 0xC1.toByte))
      spannerInteractions.upsertObject(key, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(key).futureValue should be(Result(value, serId, serManifest, 0L))
    }
    "produce an error when fetching a non-existing key" in {
      spannerInteractions.getObject("nonexistent-key").failed.futureValue.getMessage should include(
        "No data found for key [nonexistent-key]"
      )
    }
    "update a value" in {
      val key = "key-to-be-updated"
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(key, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(key).futureValue should be(Result(value, serId, serManifest, 0L))

      val updatedValue = ByteString("Open to Feedback")
      spannerInteractions.upsertObject(key, serId, serManifest, updatedValue, 1L).futureValue
      spannerInteractions.getObject(key).futureValue should be(Result(updatedValue, serId, serManifest, 1L))
    }
    "support deletions" in {
      val key = "to-be-added-and-removed"
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(key, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(key).futureValue should be(Result(value, serId, serManifest, 0L))
      spannerInteractions.deleteObject(key).futureValue
      spannerInteractions.getObject(key).failed.futureValue.getMessage should include(
        s"No data found for key [$key]"
      )
    }
  }
}
