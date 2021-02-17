/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.annotation.InternalStableApi
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerObjectInteractions}
import akka.util.ByteString

import scala.util.control.NoStackTrace
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
      entity: String,
      key: String,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long
  ): Future[Unit] = interactions.upsertObject(entity, key, serId, serManifest, value, seqNr)

  def getObject(key: String): Future[Result] = interactions.getObject(key)

  def deleteObject(key: String): Future[Unit] = interactions.deleteObject(key)
}

/**
 * This API would eventually evolve into the akka-persistence object API
 *
 * INTERNAL API
 */
object SpannerObjectStore {
  case class ObjectNotFound(key: String) extends RuntimeException(s"No data found for key [$key]") with NoStackTrace

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
