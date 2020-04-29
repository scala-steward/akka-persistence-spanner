/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, PostStop, PreRestart, Signal}
import akka.actor.typed.scaladsl.LoggerOps
import akka.util.PrettyDuration._
import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool._
import com.google.spanner.v1._
import io.grpc.StatusRuntimeException

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 *
 * INTERNAL API
 *
 * Sessions are used to execute transactions
 * They are expensive so should be re-used.
 *
 * See https://github.com/akka/akka-persistence-spanner/issues/12 for future features
 *
 * See google advice for session pools: https://cloud.google.com/spanner/docs/sessions#create_and_size_the_session_cache
 *
 */
@InternalApi
private[spanner] object SessionPool {
  sealed trait Command
  final case class GetSession(replyTo: ActorRef[Response], id: Long) extends Command
  final case class ReleaseSession(id: Long, recreate: Boolean) extends Command

  private final case class InitialSessions(sessions: List[Session]) extends Command
  private final case class RetrySessionCreation(in: FiniteDuration) extends Command
  private final case object CreateSingleSession extends Command
  private final case class SessionCreated(session: Session) extends Command
  private final case class SessionCreateFailed(t: Throwable) extends Command
  private case object KeepAlive extends Command
  private case object Stats extends Command

  sealed trait Response {
    val id: Long
  }
  final case class PooledSession(session: Session, id: Long) extends Response
  final case class PoolBusy(id: Long) extends Response

  private final case class SessionKeepAliveSuccess(session: Session) extends Command
  private final case class SessionKeepAliveFailure(session: Session, cause: Throwable) extends Command

  private final case class AvailableSession(session: Session, lastUsed: Long)

  def apply(client: SpannerClient, settings: SpannerSettings): Behavior[SessionPool.Command] =
    Behaviors.withStash[Command](settings.sessionPool.maxOutstandingRequests) { stash =>
      Behaviors.withTimers[Command] { timers =>
        Behaviors.setup[Command] { ctx =>
          ctx.log.info(
            "Creating pool. Max size [{}]. Stash [{}].",
            settings.sessionPool.maxSize,
            settings.sessionPool.maxOutstandingRequests
          )
          def createSessions(): Unit = {
            ctx.log.debug(
              "Creating sessions, database [{}], pool size [{}]",
              settings.fullyQualifiedDatabase,
              settings.sessionPool.maxSize
            )
            ctx.pipeToSelf(
              client.batchCreateSessions(
                BatchCreateSessionsRequest(settings.fullyQualifiedDatabase, sessionCount = settings.sessionPool.maxSize)
              )
            ) {
              case Success(response) => InitialSessions(response.session.toList)
              case Failure(t) =>
                ctx.log.warn("Session creation failed. Retrying ", t)
                RetrySessionCreation(settings.sessionPool.retryCreateInterval)
            }
          }

          createSessions()

          Behaviors.receiveMessagePartial {
            case InitialSessions(sessions) =>
              ctx.log.debug("Sessions created [{}]", sessions)
              timers.startTimerWithFixedDelay(KeepAlive, settings.sessionPool.keepAliveInterval)
              // FIXME Make configurable https://github.com/akka/akka-persistence-spanner/issues/42
              timers.startTimerWithFixedDelay(Stats, 1.second)
              stash.unstashAll(
                new SessionPool(client, sessions, ctx, timers, stash, settings)
              )
            case RetrySessionCreation(when) =>
              if (when == Duration.Zero) {
                ctx.log.debug("Retrying session creation")
                createSessions()
              } else {
                timers.startSingleTimer(RetrySessionCreation(Duration.Zero), when)
              }
              Behaviors.same
            case gt @ GetSession(replyTo, id) =>
              if (stash.isFull) {
                ctx.log.warn("Session pool request stash full, denying request for pool while starting up")
                replyTo ! PoolBusy(id)
              } else {
                stash.stash(gt)
              }
              Behaviors.same
          }
        }
      }
    }
}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] final class SessionPool(
    client: SpannerClient,
    initialSessions: List[Session],
    ctx: ActorContext[Command],
    timers: TimerScheduler[Command],
    stash: StashBuffer[Command],
    settings: SpannerSettings
) extends AbstractBehavior[SessionPool.Command](ctx) {
  private val keepAliveInMillis = settings.sessionPool.keepAliveInterval.toMillis
  private val log = ctx.log
  private var availableSessions: mutable.Queue[AvailableSession] =
    mutable.Queue(initialSessions.map(AvailableSession(_, System.currentTimeMillis())): _*)
  private var inUseSessions = Map.empty[Long, Session]
  private var uses = 0

  override def onMessage(msg: Command): Behavior[Command] = msg match {
    case gt @ GetSession(replyTo, id) =>
      if (log.isTraceEnabled()) {
        log.traceN(
          "GetSession [{}] from [{}], inUseSessions [{}], availableSessions [{}] stashed [{}]",
          id,
          replyTo,
          inUseSessions.mkString(", "),
          availableSessions.map(a => (a.session.name, a.lastUsed)).mkString(", "),
          stash.size
        )
      }
      if (availableSessions.nonEmpty) {
        val next = availableSessions.dequeue()
        replyTo ! PooledSession(next.session, id)
        inUseSessions += (id -> next.session)
      } else {
        if (stash.isFull) {
          ctx.log.warn("Session pool request stash full, denying request for pool")
          replyTo ! PoolBusy(id)
        } else {
          ctx.log.debug("Stashing request {}", id)
          stash.stash(gt)
        }
      }
      this
    case ReleaseSession(id, shouldRecreate) =>
      log.trace("ReleaseSession {} stash size {}", id, stash.size)
      uses += 1
      if (inUseSessions.contains(id)) {
        val session = inUseSessions(id)
        inUseSessions -= id
        if (!shouldRecreate) {
          availableSessions.enqueue(AvailableSession(session, System.currentTimeMillis()))
          stash.unstash(this, 1, identity)
        } else {
          log.debug("Session released with should re-create. Not returning this session to the pool")
          ctx.self ! CreateSingleSession
          this
        }
      } else {
        // FIXME, this may be that it timed out and is in the stash waiting, need
        // to deal with that case and not send a session
        // https://github.com/akka/akka-persistence-spanner/issues/42
        log.error("unknown session returned [{}]. This is a bug.", id)
        if (log.isDebugEnabled) {
          log.debugN(
            "In-use sessions [{}]. Available sessions [{}]",
            inUseSessions.map { case (id, session) => (id, session.name) },
            availableSessions.map { case AvailableSession(session, lastUsed) => (session.name, lastUsed) }
          )
        }
        this
      }
    case KeepAlive =>
      val currentTime = System.currentTimeMillis()
      val toKeepAlive = availableSessions.collect {
        case AvailableSession(session, lastUse) if (currentTime - lastUse) > keepAliveInMillis =>
          session
      }

      if (toKeepAlive.nonEmpty) {
        if (log.isDebugEnabled) {
          log.debugN(
            "The following sessions haven't been used in the last {}. Sending keep alive. [{}]",
            settings.sessionPool.keepAliveInterval.pretty,
            toKeepAlive.map(_.name)
          )
        }
        availableSessions = availableSessions.filterNot(s => toKeepAlive.contains(s.session))

        toKeepAlive.foreach { session =>
          ctx.pipeToSelf(client.executeSql(ExecuteSqlRequest(session.name, sql = "SELECT 1"))) {
            case Success(_) => SessionKeepAliveSuccess(session)
            case Failure(t) => SessionKeepAliveFailure(session, t)
          }
        }
      }
      this

    case SessionKeepAliveSuccess(session) =>
      availableSessions.enqueue(AvailableSession(session, System.currentTimeMillis()))
      stash.unstash(this, 1, identity)
      this

    case SessionKeepAliveFailure(session, cause) =>
      cause match {
        case t: StatusRuntimeException if t.getStatus.getCode == SessionDeleted.statusCode =>
          log.warn(
            "Keep alive of session failed with NOT_FOUND. Re-creating session and adding new instance to pool.",
            t
          )
          context.self ! CreateSingleSession
        case _ =>
          log.warn(
            s"Failed to keep session [${session.name}] alive, may be re-tried again before expires server side. " +
            s"Only Status.NOT_FOUND causes a session to be re-created.",
            cause
          )
          availableSessions.enqueue(AvailableSession(session, System.currentTimeMillis()))
      }
      stash.unstash(this, 1, identity)
      this

    case CreateSingleSession =>
      context.pipeToSelf(client.createSession(CreateSessionRequest(settings.fullyQualifiedDatabase))) {
        case Success(s) => SessionCreated(s)
        case Failure(t) => SessionCreateFailed(t)
      }
      this

    case SessionCreated(s) =>
      log.debug("Replacement session created {}", s)
      availableSessions.enqueue(AvailableSession(s, System.currentTimeMillis()))
      stash.unstash(this, 1, identity)
      this

    case SessionCreateFailed(t) =>
      log.warn("Failed to create replacement session. It will be retried.", t)
      timers.startSingleTimer(CreateSingleSession, settings.sessionPool.retryCreateInterval)
      this

    case Stats =>
      // improve this in https://github.com/akka/akka-persistence-spanner/issues/51
      log.infoN(
        "in use {}. available {}. used since last stats: {}. Ids {}. Stash size {}",
        inUseSessions.size,
        availableSessions.size,
        uses,
        inUseSessions.keys,
        stash.size
      )
      uses = 0
      this

    case other =>
      log.error("Unexpected message in active state [{}]", other.getClass)
      this
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      cleanupOldSessions()
      Behaviors.same
    case PreRestart =>
      cleanupOldSessions()
      Behaviors.same
  }

  private def cleanupOldSessions(): Unit =
    (availableSessions.map(_.session) ++ inUseSessions.values).foreach { session =>
      client.deleteSession(DeleteSessionRequest(session.name))
    }
}
