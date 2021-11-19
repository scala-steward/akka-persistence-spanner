/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.state.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.jdk.FutureConverters.FutureOps
import scala.jdk.OptionConverters._
import scala.concurrent.ExecutionContext
import akka.Done
import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.{DurableStateStorePagedPersistenceIdsQuery, DurableStateStoreQuery}
import akka.persistence.state.javadsl.DurableStateUpdateStore
import akka.persistence.state.javadsl.GetObjectResult
import akka.persistence.spanner.state.scaladsl.{SpannerDurableStateStore => ScalaSpannerDurableStateStore}
import akka.stream.javadsl.Source

object SpannerDurableStateStore {
  val Identifier = ScalaSpannerDurableStateStore.Identifier
}

/**
 * API May Change
 */
@ApiMayChange
class SpannerDurableStateStore[A](scalaStore: akka.persistence.spanner.state.scaladsl.SpannerDurableStateStore[A])(
    implicit ec: ExecutionContext
) extends DurableStateUpdateStore[A]
    with DurableStateStoreQuery[A]
    with DurableStateStorePagedPersistenceIdsQuery[A] {
  def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
    scalaStore
      .getObject(persistenceId)
      .map(x => GetObjectResult(Optional.ofNullable(x.value.getOrElse(null.asInstanceOf[A])), x.revision))
      .asJava

  def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): CompletionStage[Done] =
    scalaStore.upsertObject(persistenceId, revision, value, tag).asJava

  def deleteObject(persistenceId: String): CompletionStage[Done] =
    scalaStore.deleteObject(persistenceId).asJava

  def currentChanges(tag: String, offset: Offset): Source[DurableStateChange[A], NotUsed] =
    scalaStore.currentChanges(tag, offset).asJava

  def changes(tag: String, offset: Offset): Source[DurableStateChange[A], NotUsed] =
    scalaStore.changes(tag, offset).asJava

  override def currentPersistenceIds(afterId: Optional[String], limit: Long): Source[String, NotUsed] =
    scalaStore.currentPersistenceIds(afterId.toScala, limit).asJava
}
