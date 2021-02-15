/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.actor.ActorSystem
import akka.annotation.InternalStableApi
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SpannerObjectInteractions.{ObjectNotFound, Result}
import akka.util.ByteString
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{ListValue, Value}
import com.google.spanner.v1.{KeySet, Mutation, ReadRequest, TypeCode}

import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalStableApi
object SpannerObjectInteractions {
  case class ObjectNotFound(key: String) extends Exception(s"No data found for key [$key]")

  case class Result(byteString: ByteString, serId: Long, serManifest: String, seqNr: Long)

  object Schema {
    // To back Durable Actors
    object Objects {
      def objectTable(settings: SpannerSettings): String = objectTable(settings.objectTable)
      def objectTable(table: String): String =
        s"""CREATE TABLE $table (
           |  entity STRING(MAX) NOT NULL,
           |  key STRING(MAX) NOT NULL,
           |  value BYTES(MAX),
           |  ser_id INT64 NOT NULL,
           |  ser_manifest STRING(MAX) NOT NULL,
           |  seq_nr INT64 NOT NULL,
           |  write_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
           |) PRIMARY KEY (key)""".stripMargin

      // Constant over all instances of this entity
      val Entity = "entity" -> TypeCode.STRING
      // Key that uniquely identifies an entity instance - typically by concatenating the entity field and a unique entityId
      val Key = "key" -> TypeCode.STRING
      val Value = "value" -> TypeCode.BYTES
      val SerId = "ser_id" -> TypeCode.INT64
      val SerManifest = "ser_manifest" -> TypeCode.STRING
      val SeqNr = "seq_nr" -> TypeCode.INT64
      val WriteTime = "write_time" -> TypeCode.TIMESTAMP

      val Columns = List(Entity, Key, SerId, SerManifest, Value, SeqNr, WriteTime)
    }
  }
}

/**
 * INTERNAL API
 *
 * Class for doing spanner interaction outside of an actor to avoid mistakes
 * in future callbacks
 */
@InternalStableApi
final class SpannerObjectInteractions(
    spannerGrpcClient: SpannerGrpcClient,
    settings: SpannerSettings
)(
    implicit ec: ExecutionContext,
    system: ActorSystem
) {
  import SpannerObjectInteractions.Schema.Objects

  def upsertObject(
      entity: String,
      key: String,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long
  ): Future[Unit] =
    spannerGrpcClient.withSession(session => {
      spannerGrpcClient
        .write(Seq(Mutation(upsertObjectOperation(entity, key, serId, serManifest, value, seqNr))))(session)
    })

  // TODO maybe return timestamp?
  def getObject(key: String): Future[Result] =
    spannerGrpcClient
      .withSession(
        session =>
          spannerGrpcClient.client.read(
            ReadRequest(
              session.session.name,
              transaction = None,
              settings.objectTable,
              index = "",
              Seq(Objects.Value, Objects.SerId, Objects.SerManifest, Objects.SeqNr).map(_._1),
              wrapKey(key),
              limit = 1
            )
          )
      )
      .map(
        resultSet =>
          resultSet.rows.headOption match {
            case Some(ListValue(Seq(value, serId, serManifest, seqNr), _)) =>
              Result(
                ByteString(value.getStringValue).decodeBase64,
                serId.getStringValue.toLong,
                serManifest.getStringValue,
                seqNr.getStringValue.toLong
              )
            case None =>
              throw new ObjectNotFound(key)
          }
      )

  def deleteObject(key: String): Future[Unit] =
    spannerGrpcClient.withSession(session => {
      spannerGrpcClient.write(
        Seq(
          Mutation(
            Mutation.Operation.Delete(
              Mutation.Delete(
                settings.objectTable,
                wrapKey(key)
              )
            )
          )
        )
      )(session)
    })

  private def wrapKey(key: String) = Some(KeySet(List(ListValue(List(wrapValue(Objects.Key, key))))))

  private object CommitTimestamp

  private def upsertObjectOperation(
      entity: String,
      key: String,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long
  ): Mutation.Operation =
    Mutation.Operation.InsertOrUpdate(
      Mutation.Write(
        settings.objectTable,
        Objects.Columns.map(_._1),
        List(ListValue(Objects.Columns.map {
          case Objects.Entity => wrapValue(Objects.Entity, entity)
          case Objects.Key => wrapValue(Objects.Key, key)
          case Objects.SerId => wrapValue(Objects.SerId, serId)
          case Objects.SerManifest => wrapValue(Objects.SerManifest, serManifest)
          case Objects.SeqNr => wrapValue(Objects.SeqNr, seqNr)
          case Objects.Value => wrapValue(Objects.Value, value)
          case Objects.WriteTime => wrapValue(Objects.WriteTime, CommitTimestamp)
          case other => throw new MatchError(other)
        }))
      )
    )

  private def wrapValue(column: (String, TypeCode), value: Any): Value =
    Value((column, value) match {
      case ((_, TypeCode.STRING), stringValue: String) => StringValue(stringValue)
      case ((_, TypeCode.INT64), longValue: Long) => StringValue(longValue.toString)
      case ((_, TypeCode.BYTES), bytes: ByteString) =>
        // the protobuf struct data type doesn't have a type for binary data, so we base64-encode it
        // https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/struct.proto#L62
        StringValue(bytes.encodeBase64.utf8String)
      case ((_, TypeCode.TIMESTAMP), CommitTimestamp) =>
        StringValue("spanner.commit_timestamp()")
      case ((name, kind), value) =>
        if (value == null)
          throw new IllegalStateException(s"Cannot wrap null value of type for column [$name] of type [$kind]")
        else
          throw new IllegalStateException(
            s"Cannot wrap value of type [${value.getClass}] for column [$name] of type [$kind]"
          )
    })
}
