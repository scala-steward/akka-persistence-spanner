/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.SpannerObjectStore.{ObjectNotFound, Result}
import akka.util.ByteString
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{ListValue, Struct, Value}
import com.google.spanner.v1.{KeySet, Mutation, ReadRequest, Type, TypeCode}
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi
object SpannerObjectInteractions {
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
           |  sequence_nr INT64 NOT NULL,
           |  write_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
           |) PRIMARY KEY (key)""".stripMargin

      /** Columns */
      // Constant over all instances of this entity
      val Entity = "entity" -> TypeCode.STRING
      // Key that uniquely identifies an entity instance - typically by concatenating the entity field and a unique entityId
      val Key = "key" -> TypeCode.STRING
      val Value = "value" -> TypeCode.BYTES
      val SerId = "ser_id" -> TypeCode.INT64
      val SerManifest = "ser_manifest" -> TypeCode.STRING
      val SeqNr = "sequence_nr" -> TypeCode.INT64
      val WriteTime = "write_time" -> TypeCode.TIMESTAMP

      val Columns = List(Entity, Key, SerId, SerManifest, Value, SeqNr, WriteTime)

      /** For use in queries: */
      val OldSeqNr = "old_sequence_nr" -> TypeCode.INT64
      val NewSeqNr = "new_sequence_nr" -> TypeCode.INT64
    }
  }
}

/**
 * INTERNAL API
 *
 * Class for doing spanner interaction outside of an actor to avoid mistakes
 * in future callbacks
 */
@InternalApi
private[spanner] final class SpannerObjectInteractions(
    spannerGrpcClient: SpannerGrpcClient,
    settings: SpannerSettings
)(
    implicit ec: ExecutionContext,
    system: ActorSystem
) {
  import SpannerObjectInteractions.Schema.Objects

  val SqlUpdate =
    s"UPDATE ${settings.objectTable} SET value = @value, ser_id = @ser_id, ser_manifest = @ser_manifest, " +
    s"sequence_nr = @new_sequence_nr, write_time = PENDING_COMMIT_TIMESTAMP() " +
    s"WHERE key = @key AND sequence_nr = @old_sequence_nr"
  val SqlUpdateTypes: Map[String, TypeCode] =
    List(
      Objects.Value,
      Objects.SerId,
      Objects.SerManifest,
      Objects.NewSeqNr,
      Objects.Key,
      Objects.OldSeqNr
    ).toMap

  def upsertObject(
      entity: String,
      key: String,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long
  ): Future[Unit] =
    spannerGrpcClient.withSession(session => {
      if (seqNr == 0) insertFirst(entity, key, serId, serManifest, value, seqNr, session)
      else update(entity, key, serId, serManifest, value, seqNr, session)
    })

  private object CommitTimestamp

  private def insertFirst(
      entity: String,
      key: String,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long,
      session: SessionPool.PooledSession
  ) =
    spannerGrpcClient
      .write(
        Seq(
          Mutation(
            Mutation.Operation.Insert(
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
          )
        )
      )(session)
      .recoverWith {
        case e: StatusRuntimeException =>
          if (e.getStatus.getCode == Status.Code.ALREADY_EXISTS)
            Future.failed(new IllegalStateException(s"Insert failed: object for key [$key] already exists"))
          else
            Future.failed(e)
      }

  private def update(
      entity: String,
      key: String,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long,
      session: SessionPool.PooledSession
  ): Future[Unit] =
    spannerGrpcClient
      .executeBatchDml(
        List(
          (SqlUpdate, Struct(SqlUpdateTypes.map {
            case Objects.Entity => wrapNameValue(Objects.Entity, entity)
            case Objects.Key => wrapNameValue(Objects.Key, key)
            case Objects.SerId => wrapNameValue(Objects.SerId, serId)
            case Objects.SerManifest => wrapNameValue(Objects.SerManifest, serManifest)
            case Objects.Value => wrapNameValue(Objects.Value, value)
            case Objects.OldSeqNr => wrapNameValue(Objects.OldSeqNr, seqNr - 1)
            case Objects.NewSeqNr => wrapNameValue(Objects.NewSeqNr, seqNr)
            case other => throw new MatchError(other)
          }), SqlUpdateTypes.mapValues(Type(_)).toMap)
        )
      )(session)
      .map(response => {
        val updated = response.resultSets.head.stats.head.rowCount.rowCountExact.head
        if (updated != 1L)
          throw new IllegalStateException(
            s"Update failed: object for key [$key] could not be updated to sequence number [$seqNr]"
          )
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
              throw ObjectNotFound(key)
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

  private def wrapNameValue(column: (String, TypeCode), value: Any): (String, Value) =
    (column._1, wrapValue(column, value))

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
