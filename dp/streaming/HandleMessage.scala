package com.bhp.dp.streaming

import com.azure.messaging.servicebus._
import com.azure.messaging.servicebus.models.DeadLetterOptions
import com.bhp.dp.config.ConfigHelper
import com.bhp.dp.logging.LoggingConstants
import com.bhp.dp.utils.{MessageUtils, MsTeamsUtil}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.apache.logging.log4j.{CloseableThreadContext, ThreadContext}
import org.apache.logging.log4j.scala.Logging
import util.retry.blocking.{Failure => RetryFailure, Retry, RetryStrategy, Success => RetrySuccess}
import util.retry.blocking.RetryStrategy.RetryStrategyProducer

import java.time.{Duration, OffsetDateTime, ZoneId}
import java.time.format.{DateTimeFormatter, FormatStyle}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try, Using}

/** Base class for handling a `ServiceBusReceivedMessage` Handles querying the client for new
  * messages, locking the message, converting it to an EventGridMessage, and then Finishing
  * (Complete, DeadLetter, Abandon)
  *
  * This is an abstract class because at one time there were multiple messages types and now that
  * there is only one it isn't worth the effort to coalesce the single concrete class into this
  * abstract one. (and it would preclude any future needs of multiple message types)
  *
  * @param hasBeenCancelled
  *   AtomicBoolean as to whether the job has been cancelled
  * @param client
  *   The ServiceBusReceiverClient. Used to Complete, Abandon, or Deadletter messages
  * @param senderClient
  *   The ServiceBusSenderClient. Used to send Delayed messages
  * @param messageLockExecutionContext
  *   ExecutionContext to hold threads to lock messages
  * @param msTeamsUtil
  *   msTeamsUtil used to send error messages to teams
  * @param concurrentMessageCount
  *   Max number of messages being processed at once
  * @param maxLastMessageDuration
  *   Optional param that if specified limits the amount of time this will listen without getting a
  *   message. Will warn and end if the time limit is exceeded.
  */
abstract class HandleMessage(
    hasBeenCancelled: AtomicBoolean,
    val client: ServiceBusReceiverClient,
    val senderClient: ServiceBusSenderClient,
    messageLockExecutionContext: ExecutionContext,
    msTeamsUtil: MsTeamsUtil,
    concurrentMessageCount: Int,
    maxLastMessageDuration: Option[Duration] = None
) extends Logging {

  import HandleMessage._

  private implicit val ec: ExecutionContext = messageLockExecutionContext

  private val workLock = new Object()

  private val runningLock = new Object()
  private val runningIngests: mutable.Map[String, Future[String]] =
    new java.util.concurrent.ConcurrentHashMap[String, Future[String]]().asScala

  private val outsideValues = ThreadContext.getImmutableContext

  protected def handleEventGridMessage(
      message: EventGridMessage,
      metadata: MessageMetadata,
      ctx: CloseableThreadContext.Instance
  ): Outcome

  /** Optional method to handle a non-ServiceBus related Throwable Return true if logged/sent to
    * teams. Returns None if it doesn't handle the exception. Returns Some(exception) for that
    * exception to be the deadletter reason
    * @param t
    * @return
    */
  protected def handleThrowable(t: Throwable): Option[Throwable] = None

  private implicit val retryStrategy: RetryStrategyProducer =
    RetryStrategy.fibonacciBackOff(initialWaitDuration = 10.seconds, maxAttempts = 5)

  def run(): Unit = {
    while (!hasBeenCancelled.get()) {
      val messages = Retry(client.receiveMessages(1)) match {
        case RetrySuccess(m)         => m
        case RetryFailure(exception) => throw exception
      }

      messages.asScala.foreach(m => {
        recentMessageHistory.add(MessageRecord(m.getSubject, m.getEnqueuedTime))
        Using(CloseableThreadContext.putAll(outsideValues)) { ctx =>
          {
            ctx.put(LoggingConstants.INGEST_ID, m.getMessageId)
            try {
              handleMessage(m)
            } catch {
              // this will be very rare to non-existent. basically everything inside handleMessage is surrounded by try/catch
              // but we can't trust that always be the case, so try/catch it is!
              case e: Exception =>
                logger.error("Exception while handling message", e)
                msTeamsUtil.sendGenericError(e, "Exception while handling message")
            }
          }
        }
      })

      maxLastMessageDuration match {
        case Some(maxDuration) =>
          val now = OffsetDateTime.now()
          val lastMessage = if (recentMessageHistory.isEmpty) {
            startupTime
          } else {
            recentMessageHistory.get(recentMessageHistory.size() - 1).timestamp
          }

          if (Duration.between(lastMessage, now).compareTo(maxDuration) > 0) {
            // it has been longer than maxDuration. warn and set cancel to end.
            val formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM)
            val extraInfo = recentMessageHistory
              .iterator()
              .asScala
              .zipWithIndex
              .foldLeft(new StringBuilder()) { case (sb, (record, idx)) =>
                val cleanSubject =
                  Try(ConfigHelper.cleanBlobSubject(record.subject)).getOrElse(record.subject)

                val messageEnqueued = record.messageTimestamp.format(formatter)
                val messageSeen     = record.timestamp.format(formatter)
                sb.append(
                  s"${idx + 1} from ${messageEnqueued}Z seen at ${messageSeen}Z -> $cleanSubject\n"
                )
              }
              .toString()
              .trim

            hasBeenCancelled.synchronized {
              hasBeenCancelled.set(true)
              hasBeenCancelled.notifyAll()
            }
          }
        case None => // maxDuration not set. do not check last seen
      }
      runningLock.synchronized {
        while (runningIngests.size >= concurrentMessageCount) {
          try {
            runningLock.wait(15.seconds.toMillis)
          } catch {
            case _: InterruptedException => // ignore
          }
        }
      }
    }
  }

  private def handleMessage(m: ServiceBusReceivedMessage): Unit = {
    val messageBody = m.getBody.toString

    logger.info(s"Received message with body $messageBody")

    if (!hasBeenCancelled.get()) {
      workLock.synchronized {

        val messageIdLock = lockCache.get(m.getMessageId, () => new AtomicBoolean(false))

        messageIdLock.synchronized {
          if (messageIdLock.get()) {
            logger.warn(s"${m.getMessageId} was already processed. Skipping.")
            try {
              client.retryingComplete(m)
            } catch {
              case e: Exception =>
                logger.error(s"Exception while completing skipped message", e)
                msTeamsUtil.sendGenericError(e, "Exception while completing skipped message.")
            }
          } else {
            try {
              val eventGridMessage = MessageUtils.tryParseEventGridMessage(messageBody)
              eventGridMessage match {
                case Failure(exception) =>
                  logger.error(s"Could not parse message $messageBody", exception)
                  msTeamsUtil.sendGenericError(exception, s"Could not parse message $messageBody")
                  client.retryingDeadLetter(m, s"Could not parse message")

                case Success(message) =>
                  val workComplete = new AtomicBoolean(false)
                  launchRefreshTokenFuture(m, workComplete)

                  val currentValues: util.Map[String, String] = ThreadContext.getImmutableContext
                  val processFuture = Future {
                    Using(CloseableThreadContext.putAll(currentValues)) { ctx =>
                      {
                        try {
                          val messageMetadata = MessageMetadata(m)
                          val outcome = handleEventGridMessage(message, messageMetadata, ctx)

                          outcome match {
                            case Outcome.Complete =>
                              messageIdLock.set(true)
                              client.retryingComplete(m)
                            case Outcome.DeadLetter(reason) =>
                              messageIdLock.set(true)
                              client.retryingDeadLetter(m, reason)
                            case Outcome.Abandon            => client.retryingAbandon(m)
                            case Outcome.Delay(delayAmount) =>
                              // create a new message and send as scheduled

                              val outMessage = new ServiceBusMessage(m)
                              val delayCount = messageMetadata.delayCount + 1
                              outMessage.getApplicationProperties.put("delayCount", s"$delayCount")
                              // duplicate detection is based ONLY on the message id. we have to set it to a new id before sending.
                              val newMessageId = UUID.randomUUID().toString
                              outMessage.setMessageId(newMessageId)
                              // this MUST be UTC or Azure misinterprets the scheduled time
                              val scheduledTime =
                                OffsetDateTime.now(ZoneId.of("UTC")).plus(delayAmount)
                              logger.info(
                                s"Scheduling message for $scheduledTime with delayCount $delayCount. New message id is $newMessageId."
                              )
                              senderClient.retryingSchedule(outMessage, scheduledTime)

                              // complete the current message
                              client.retryingComplete(m)
                          }

                          m.getMessageId
                        } catch {
                          case t: Throwable => throw new HandleMessageException(m.getMessageId, t)
                        } finally {
                          // close down the renew message lock Future
                          workComplete.synchronized {
                            workComplete.set(true)
                            workComplete.notifyAll()
                          }
                        }
                      }
                    }.get
                  }

                  runningIngests.put(m.getMessageId, processFuture)

                  addOnComplete(m, processFuture, currentValues)
              }
            } catch {
              case sb: ServiceBusException =>
                // do not try to deadletter as it will throw another exception
                logger.error(s"Exception finishing message", sb)
                msTeamsUtil.sendGenericError(sb, "Service Bus exception during processing.")
              case e: Throwable =>
                logger.error(s"Exception while processing", e)
                msTeamsUtil.sendGenericError(e, "Exception while processing")
                client.retryingDeadLetter(m, s"Exception while processing: ${e.getMessage}")
            }
          }
        }
      }
    } else {
      try {
        client.retryingAbandon(m)
      } catch {
        case e: Exception =>
          logger.error(s"Exception while abandoning message after cancel", e)
          msTeamsUtil.sendGenericError(
            e,
            "Exception abandoning message after RawLoader job cancelled."
          )
      }
    }
  }

  private def addOnComplete(
      m: ServiceBusReceivedMessage,
      processFuture: Future[String],
      currentContextValues: util.Map[String, String]
  ): Unit = {
    processFuture.onComplete(attempt => {
      Using(CloseableThreadContext.putAll(currentContextValues)) { _ =>
        {
          attempt match {
            case Success(messageId) =>
              runningLock.synchronized {
                runningIngests.remove(messageId)
                runningLock.notifyAll()
              }
            case Failure(exception) =>
              val hme = exception.asInstanceOf[HandleMessageException]

              runningLock.synchronized {
                runningIngests.remove(hme.messageId)
                runningLock.notifyAll()
              }

              hme.getCause match {
                case sb: ServiceBusException =>
                  // do not try to deadletter as it will throw another exception
                  logger.error(s"Exception finishing message", sb)
                  msTeamsUtil.sendGenericError(sb, "Service Bus exception during processing.")
                case e: Throwable =>
                  val dlEx = handleThrowable(e) match {
                    case Some(value) => value
                    case None =>
                      logger.error(s"Exception while processing", e)
                      msTeamsUtil.sendGenericError(e, "Exception while processing file")
                      e
                  }

                  client.retryingDeadLetter(
                    m,
                    s"Exception while processing file: ${dlEx.getMessage}"
                  )
              }
          }
        }
      }

    })
  }

  private def launchRefreshTokenFuture(
      m: ServiceBusReceivedMessage,
      workComplete: AtomicBoolean
  ): Future[Unit] = {
    Future {
      Using(CloseableThreadContext.put(LoggingConstants.INGEST_ID, m.getMessageId))(_ => {
        var failureCount = 0
        logger.info(s"Starting lock renew on token ${m.getLockToken}")
        while (!workComplete.get()) {
          try {
            workComplete.synchronized {
              if (!workComplete.get()) {
                client.renewMessageLock(m)
                workComplete.wait(45.seconds.toMillis)
              }
            }
          } catch {
            case _: InterruptedException => // do nothing
            case e: Exception =>
              failureCount += 1
              if (failureCount > 5) {
                logger.error(
                  s"Failed lock renew on token ${m.getLockToken} more than 5 times",
                  e
                )
                throw e
              } else {
                logger.warn(
                  s"Failed lock renew on token ${m.getLockToken} $failureCount times",
                  e
                )
              }
          }
        }
        logger.info(s"Finished lock renew on token ${m.getLockToken}")
      })
    }
  }

  /** Wait for work to finish
    */
  def waitForFinalWork(): Unit = {
    workLock.synchronized {
      runningLock.synchronized {
        while (runningIngests.nonEmpty) {
          runningLock.wait(30.seconds.toMillis)
        }
      }
    }
  }
}

class HandleMessageException(val messageId: String, t: Throwable) extends Exception(t)

sealed trait Outcome

object Outcome {
  object Complete extends Outcome

  object Abandon extends Outcome

  case class DeadLetter(reason: String) extends Outcome

  case class Delay(delayAmount: Duration) extends Outcome
}

case class MessageMetadata(messageId: String, enqueuedTime: OffsetDateTime, delayCount: Int = 0)

object MessageMetadata {
  def apply(m: ServiceBusReceivedMessage): MessageMetadata = {
    val properties = m.getApplicationProperties.asScala
    val delayCount = properties.getOrElse("delayCount", "0").asInstanceOf[String].toInt

    new MessageMetadata(m.getMessageId, m.getEnqueuedTime, delayCount)
  }
}

object HandleMessage {
  val startupTime: OffsetDateTime = OffsetDateTime.now()

  // this cache is used to validate that we do not "do work" for the same message id twice
  val lockCache: Cache[String, AtomicBoolean] = CacheBuilder
    .newBuilder()
    .maximumSize(500)
    .concurrencyLevel(1)
    .build()

  case class MessageRecord(subject: String, messageTimestamp: OffsetDateTime) {
    val timestamp: OffsetDateTime = OffsetDateTime.now(ZoneId.of("UTC"))
  }

  val recentMessageHistory: CircularFifoQueue[MessageRecord] =
    new CircularFifoQueue[MessageRecord](10)

  implicit class DurationPrettyPrint(duration: Duration) {
    def prettyPrint: String =
      duration.toString.substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").toLowerCase()
  }

  implicit class ScalaDurationToJavaDuration(scalaSide: FiniteDuration) {
    def asJava: Duration = Duration.ofMillis(scalaSide.toMillis)
  }

  implicit class NiceSender(client: ServiceBusSenderClient) {
    private implicit val retryStrategy: RetryStrategyProducer =
      RetryStrategy.fixedBackOff(retryDuration = 5.seconds, maxAttempts = 3)

    def retryingSchedule(m: ServiceBusMessage, scheduledTime: OffsetDateTime): Unit = {
      Retry(client.scheduleMessage(m, scheduledTime)) match {
        case RetrySuccess(_)         =>
        case RetryFailure(exception) => throw exception
      }
    }
  }

  implicit class NiceReceiver(client: ServiceBusReceiverClient) {
    private implicit val retryStrategy: RetryStrategyProducer =
      RetryStrategy.fixedBackOff(retryDuration = 5.seconds, maxAttempts = 3)

    def retryingDeadLetter(m: ServiceBusReceivedMessage, reason: String): Unit = {
      val options = new DeadLetterOptions().setDeadLetterReason(reason)
      Retry(client.deadLetter(m, options)) match {
        case RetrySuccess(_)         =>
        case RetryFailure(exception) => throw exception
      }
    }

    def retryingComplete(m: ServiceBusReceivedMessage): Unit = {
      Retry(client.complete(m)) match {
        case RetrySuccess(_)         =>
        case RetryFailure(exception) => throw exception
      }
    }

    def retryingAbandon(m: ServiceBusReceivedMessage): Unit = {
      Retry(client.abandon(m)) match {
        case RetrySuccess(_)         =>
        case RetryFailure(exception) => throw exception
      }
    }
  }
}
