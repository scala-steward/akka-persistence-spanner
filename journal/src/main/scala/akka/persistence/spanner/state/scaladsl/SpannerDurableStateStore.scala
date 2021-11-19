/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.state.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try

import akka.Done
import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.scaladsl.DurableStateStoreQuery
import akka.persistence.query.scaladsl.DurableStateStorePagedPersistenceIdsQuery
import akka.persistence.spanner.internal
import akka.persistence.spanner.internal.SpannerObjectInteractions
import akka.persistence.state.scaladsl.DurableStateUpdateStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.persistence.typed.PersistenceId
import akka.serialization.Serialization
import akka.serialization.Serializers
import akka.stream.scaladsl.Source
import akka.util.ByteString

object SpannerDurableStateStore {
  val Identifier = "akka.persistence.spanner.durable-state-store"
}

/**
 * API May Change
 */
@ApiMayChange
class SpannerDurableStateStore[A](
    interactions: SpannerObjectInteractions,
    serialization: Serialization,
    implicit val executionContext: ExecutionContext
) extends DurableStateUpdateStore[A]
    with DurableStateStoreQuery[A]
    with DurableStateStorePagedPersistenceIdsQuery[A] {
  def getObject(persistenceId: String): Future[GetObjectResult[A]] =
    interactions.getObject(PersistenceId.ofUniqueId(persistenceId)).flatMap {
      case Some(r) =>
        for {
          deserialized <- Future.fromTry(
            // to Int: Spanner only has INT64
            deserialize(r.byteString.toArray, r.serId.toInt, r.serManifest)
          )
        } yield {
          GetObjectResult(Some(deserialized), r.seqNr)
        }
      case None => Future.successful(GetObjectResult(None, 0L))
    }

  def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] =
    for {
      (bytes: ByteString, serId: Int, serManifest: String) <- Future.fromTry(serialize(value))
      _ <- interactions.upsertObject(PersistenceId.ofUniqueId(persistenceId), serId, serManifest, bytes, revision, tag)
    } yield Done

  def deleteObject(persistenceId: String): Future[Done] =
    interactions.deleteObject(PersistenceId.ofUniqueId(persistenceId)).map(_ => Done)

  /**
   * Get a source of the most recent state changes with the given tag since the passed in offset.
   *
   * Note that this only returns the most recent change to each object, if an object has been updated multiple times
   * since the offset, only the most recent of those changes will be part of the stream.
   *
   * This will return changes that occurred up to when the `Source` returned by this call is materialized. Changes to
   * objects made since materialization are not guaranteed to be included in the results.
   *
   * @param tag The tag to get changes for.
   * @param offset The offset to get changes since. Must either be [[akka.persistence.query.NoOffset]] to get
   *               changes since the beginning of time, or an offset that has been previously returned by this query.
   *               Any other offsets are invalid.
   * @return A source of change events.
   */
  def currentChanges(tag: String, offset: Offset): Source[DurableStateChange[A], NotUsed] =
    interactions.currentChangesByTag(tag, offset).map(change => toDurableStateChange(change))

  /**
   * Get a source of the most recent state changes with the given tag since the passed in offset.
   *
   * The returned source will never terminate, it effectively watches for changes to the objects and emits changes as
   * they happen.
   *
   * Not all changes that occur are guaranteed to be emitted, this call only guarantees that eventually, the most
   * recent change for each object since the offset will be emitted. In particular, multiple updates to a given object
   * in quick succession are likely to be skipped, with only the last update resulting in a change event from this
   * source.
   *
   * @param tag The tag to get changes for.
   * @param offset The offset to get changes since. Must either be [[akka.persistence.query.NoOffset]] to get
   *               changes since the beginning of time, or an offset that has been previously returned by this query.
   *               Any other offsets are invalid.
   * @return A source of change events.
   */
  override def changes(tag: String, offset: Offset): Source[DurableStateChange[A], NotUsed] =
    interactions.changesByTag(tag, offset).map(change => toDurableStateChange(change))

  override def currentPersistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] =
    interactions.currentPersistenceIds(afterId, limit)

  private def toDurableStateChange(change: internal.SpannerObjectInteractions.Change): DurableStateChange[A] =
    new UpdatedDurableState[A](
      persistenceId = change.persistenceId,
      revision = change.seqNr,
      // to Int: Spanner only has INT64
      value = deserialize(change.bytes.toArray, change.serId.toInt, change.serManifest).get, // crash source if corrupt.
      offset = change.offset,
      timestamp = change.timestamp
    )

  private def serialize(payload: Any): Try[(ByteString, Int, String)] = {
    val p2 = payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(p2)
    val serManifest = Serializers.manifestFor(serializer, p2)
    val serialized = serialization.serialize(p2)
    serialized.map(payload => (ByteString(payload), serializer.identifier, serManifest))
  }

  private def deserialize(bytes: Array[Byte], serId: Int, serManifest: String) =
    serialization.deserialize(bytes, serId, serManifest).map(_.asInstanceOf[A])
}
