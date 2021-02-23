/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.SpannerObjectStore.Result
import akka.persistence.typed.PersistenceId
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
           |  entity_type STRING(MAX) NOT NULL,
           |  persistence_id STRING(MAX) NOT NULL,
           |  value BYTES(MAX),
           |  ser_id INT64 NOT NULL,
           |  ser_manifest STRING(MAX) NOT NULL,
           |  sequence_nr INT64 NOT NULL,
           |  write_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
           |) PRIMARY KEY (persistence_id)""".stripMargin

      /** Columns */
      val EntityType = "entity_type" -> TypeCode.STRING
      // Key that uniquely identifies an entity instance - typically by concatenating the entity_typef and a unique id
      val PersistenceId = "persistence_id" -> TypeCode.STRING
      val Value = "value" -> TypeCode.BYTES
      val SerId = "ser_id" -> TypeCode.INT64
      val SerManifest = "ser_manifest" -> TypeCode.STRING
      val SeqNr = "sequence_nr" -> TypeCode.INT64
      val WriteTime = "write_time" -> TypeCode.TIMESTAMP

      val Columns = List(EntityType, PersistenceId, SerId, SerManifest, Value, SeqNr, WriteTime)

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
    s"WHERE persistence_id = @persistence_id AND sequence_nr = @old_sequence_nr"
  val SqlUpdateTypes: Map[String, TypeCode] =
    List(
      Objects.Value,
      Objects.SerId,
      Objects.SerManifest,
      Objects.NewSeqNr,
      Objects.PersistenceId,
      Objects.OldSeqNr
    ).toMap

  def upsertObject(
      entity: String,
      persistenceId: PersistenceId,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long
  ): Future[Unit] =
    spannerGrpcClient.withSession(session => {
      if (seqNr == 0) insertFirst(entity, persistenceId, serId, serManifest, value, seqNr, session)
      else update(entity, persistenceId, serId, serManifest, value, seqNr, session)
    })

  private object CommitTimestamp

  private def insertFirst(
      entity: String,
      persistenceId: PersistenceId,
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
                  case Objects.EntityType => wrapValue(Objects.EntityType, entity)
                  case Objects.PersistenceId => wrapValue(Objects.PersistenceId, persistenceId)
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
            Future.failed(
              new IllegalStateException(s"Insert failed: object for persistence id [$persistenceId] already exists")
            )
          else
            Future.failed(e)
      }

  private def update(
      entity: String,
      persistenceId: PersistenceId,
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
            case Objects.EntityType => wrapNameValue(Objects.EntityType, entity)
            case Objects.PersistenceId => wrapNameValue(Objects.PersistenceId, persistenceId)
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
            s"Update failed: object for persistence id [${persistenceId.id}] could not be updated to sequence number [$seqNr]"
          )
      })

  // TODO maybe return timestamp?
  def getObject(persistenceId: PersistenceId): Future[Option[Result]] =
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
              wrapPersistenceId(persistenceId),
              limit = 1
            )
          )
      )
      .map(
        resultSet =>
          resultSet.rows.headOption.map {
            case ListValue(Seq(value, serId, serManifest, seqNr), _) =>
              Result(
                ByteString(value.getStringValue).decodeBase64,
                serId.getStringValue.toLong,
                serManifest.getStringValue,
                seqNr.getStringValue.toLong
              )
          }
      )

  def deleteObject(persistenceId: PersistenceId): Future[Unit] =
    spannerGrpcClient.withSession(session => {
      spannerGrpcClient.write(
        Seq(
          Mutation(
            Mutation.Operation.Delete(
              Mutation.Delete(
                settings.objectTable,
                wrapPersistenceId(persistenceId)
              )
            )
          )
        )
      )(session)
    })

  private def wrapPersistenceId(persistenceId: PersistenceId) =
    Some(KeySet(List(ListValue(List(wrapValue(Objects.PersistenceId, persistenceId.id))))))

  private def wrapNameValue(column: (String, TypeCode), value: Any): (String, Value) =
    (column._1, wrapValue(column, value))

  private def wrapValue(column: (String, TypeCode), value: Any): Value =
    Value((column, value) match {
      case ((_, TypeCode.STRING), stringValue: String) => StringValue(stringValue)
      case ((_, TypeCode.STRING), persistenceId: PersistenceId) => StringValue(persistenceId.id)
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
