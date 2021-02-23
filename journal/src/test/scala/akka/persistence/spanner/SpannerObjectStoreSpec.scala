/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.persistence.spanner.SpannerObjectStore.Result
import akka.persistence.typed.PersistenceId
import akka.util.ByteString

class SpannerObjectStoreSpec extends SpannerSpec("SpannerObjectStoreSpec") {
  override def withObjectStore: Boolean = true

  val spannerInteractions = SpannerObjectStore()

  // All objects in this test are carts
  val entityType = "cart"
  // All objects in this test use the same serialization ;)
  val serId = 5749231L
  val serManifest = "manifest-type-information"

  "The spanner object store" should {
    "save and retrieve a value" in {
      val persistenceId = PersistenceId(entityType, "my-persistenceId")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 0L)))
    }
    "save and retrieve a binary value" in {
      val persistenceId = PersistenceId(entityType, "my-id-for-binary-value")
      // this is not a valid UTF-8 string:
      val value = ByteString(Array[Byte](0xC0.toByte, 0xC1.toByte))
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 0L)))
    }
    "produce None when fetching a non-existing key" in {
      val key = PersistenceId(entityType, "nonexistent-id")
      spannerInteractions.getObject(key).futureValue should be(None)
    }
    "update a value" in {
      val persistenceId = PersistenceId(entityType, "id-to-be-updated")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 0L)))

      val updatedValue = ByteString("Open to Feedback")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, updatedValue, 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(
        Some(Result(updatedValue, serId, serManifest, 1L))
      )
    }
    "detect and reject concurrent inserts" in {
      val persistenceId = PersistenceId(entityType, "id-to-be-inserted-concurrently")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 0L)))

      val updatedValue = ByteString("Open to Feedback")
      val failure =
        spannerInteractions
          .upsertObject(entityType, persistenceId, serId, serManifest, updatedValue, 0L)
          .failed
          .futureValue
      failure.getMessage should include(s"Insert failed: object for persistence id [$persistenceId] already exists")
    }
    "detect and reject concurrent updates" in {
      val persistenceId = PersistenceId(entityType, "id-to-be-updated-concurrently")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 0L)))

      val updatedValue = ByteString("Open to Feedback")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, updatedValue, 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(
        Some(Result(updatedValue, serId, serManifest, 1L))
      )

      // simulate an update by a different node that didn't see the first one:
      val updatedValue2 = ByteString("Genuine and Sincere in all Communications")
      spannerInteractions
        .upsertObject(entityType, persistenceId, serId, serManifest, updatedValue2, 1L)
        .failed
        .futureValue
    }
    "support deletions" in {
      val persistenceId = PersistenceId(entityType, "to-be-added-and-removed")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, 0L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 0L)))
      spannerInteractions.deleteObject(persistenceId).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(None)
    }
  }
}
