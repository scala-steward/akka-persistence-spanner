/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.time.Instant

import akka.annotation.InternalApi
import akka.persistence.query.{NoOffset, Offset}
import akka.persistence.spanner.SpannerOffset
import akka.persistence.spanner.internal.SpannerJournalInteractions.Schema
import com.google.protobuf.struct.Value.Kind.NullValue

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerUtils {
  def unixTimestampMillisToSpanner(unixTimestampMs: Long): String =
    Instant.ofEpochMilli(unixTimestampMs).toString // I _think_ this will always be rfc3339

  def spannerTimestampToUnixMillis(spannerTimestamp: String): Long =
    Instant.parse(spannerTimestamp).toEpochMilli

  val nullValue: NullValue =
    com.google.protobuf.struct.Value.Kind.NullValue(com.google.protobuf.struct.NullValue.NULL_VALUE)

  val SpannerNoOffset = SpannerUtils.unixTimestampMillisToSpanner(0L)

  def toSpannerOffset(offset: Offset) = offset match {
    case NoOffset => SpannerOffset(SpannerNoOffset, Map.empty)
    case so: SpannerOffset => so
    case _ =>
      throw new IllegalArgumentException(s"Spanner does not support offset type: " + offset.getClass)
  }
}
