/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.annotation.{ApiMayChange, InternalStableApi}
import akka.persistence.query.Offset
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerObjectInteractions}
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Future

/**
 * This API would eventually evolve into the akka-persistence object API
 *
 * INTERNAL API
 */
@InternalStableApi
class SpannerObjectStore(interactions: SpannerObjectInteractions) {
  import SpannerObjectStore._

  def upsertObject(
      entityType: String,
      persistenceId: PersistenceId,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long
  ): Future[Unit] = interactions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr)

  def getObject(persistenceId: PersistenceId): Future[Option[Result]] = interactions.getObject(persistenceId)

  def deleteObject(persistenceId: PersistenceId): Future[Unit] = interactions.deleteObject(persistenceId)

  @ApiMayChange
  def currentChanges(entityType: String, offset: Offset): Source[Change, NotUsed] =
    interactions.currentChanges(entityType, offset)

  @ApiMayChange
  def changes(entityType: String, offset: Offset): Source[Change, NotUsed] =
    interactions.changes(entityType, offset)
}

/**
 * This API would eventually evolve into the akka-persistence object API
 *
 * INTERNAL API
 */
object SpannerObjectStore {
  case class Result(byteString: ByteString, serId: Long, serManifest: String, seqNr: Long)
  case class Change(
      persistenceId: String,
      bytes: ByteString,
      serId: Long,
      serManifest: String,
      seqNr: Long,
      offset: Offset
  )

  def apply()(implicit system: ActorSystem): SpannerObjectStore = {
    val spannerSettings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))
    val grpcClient = SpannerGrpcClientExtension(system.toTyped).clientFor("akka.persistence.spanner")
    implicit val ec = system.dispatcher
    val spannerInteractions = new SpannerObjectInteractions(
      grpcClient,
      spannerSettings
    )
    new SpannerObjectStore(spannerInteractions)
  }
}
