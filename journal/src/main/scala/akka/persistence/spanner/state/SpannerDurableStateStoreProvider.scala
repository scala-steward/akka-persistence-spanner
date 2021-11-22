/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.state

import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.ApiMayChange
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SpannerGrpcClientExtension
import akka.persistence.spanner.internal.SpannerObjectInteractions
import akka.persistence.state.DurableStateStoreProvider
import akka.persistence.state.javadsl.{DurableStateStore => JDurableStateStore}
import akka.persistence.state.scaladsl.DurableStateStore
import akka.serialization.SerializationExtension
import com.typesafe.config.Config

/**
 * API May Change
 */
@ApiMayChange
class SpannerDurableStateStoreProvider(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends DurableStateStoreProvider {
  private implicit val sys = system
  private implicit val executionContext = system.dispatcher

  private val sharedConfigPath = cfgPath.replaceAll("""\.durable-state-store$""", "")
  private val spannerSettings = new SpannerSettings(system.settings.config.getConfig(sharedConfigPath))
  private val serialization = SerializationExtension(system)

  private val grpcClient = SpannerGrpcClientExtension(system.toTyped).clientFor(sharedConfigPath)

  private val spannerInteractions = new SpannerObjectInteractions(grpcClient, spannerSettings)

  override val scaladslDurableStateStore: DurableStateStore[Any] =
    new scaladsl.SpannerDurableStateStore[Any](spannerInteractions, serialization, executionContext)

  override val javadslDurableStateStore: JDurableStateStore[AnyRef] =
    new javadsl.SpannerDurableStateStore[AnyRef](
      new scaladsl.SpannerDurableStateStore[AnyRef](spannerInteractions, serialization, executionContext)
    )
}
