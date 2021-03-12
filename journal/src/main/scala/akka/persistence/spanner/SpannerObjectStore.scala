/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.annotation.InternalStableApi
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerObjectInteractions}
import akka.persistence.typed.PersistenceId
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
}

/**
 * This API would eventually evolve into the akka-persistence object API
 *
 * INTERNAL API
 */
@InternalStableApi
object SpannerObjectStore {
  case class Result(byteString: ByteString, serId: Long, serManifest: String, seqNr: Long)

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
