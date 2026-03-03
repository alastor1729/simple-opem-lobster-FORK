package com.bhp.dp.events

import _root_.util.retry.blocking.{Failure, Retry, RetryStrategy, Success}
import _root_.util.retry.blocking.RetryStrategy.RetryStrategyProducer
import com.azure.messaging.servicebus.{ServiceBusClientBuilder, ServiceBusMessage, ServiceBusSenderClient}
import com.bhp.dp.utils.RawLoaderJobUtils
import com.bhp.dp.visiting.components.WriteInfo
import org.apache.logging.log4j.scala.Logging
import org.json4s.{DefaultFormats, Formats}
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization

import scala.concurrent.duration.DurationInt

class EventSender(serviceBusConnectionString: String, eventTopicName: String) extends Logging {

  import EventSender._

  private var eventClient: ServiceBusSenderClient = null

  private implicit val retryStrategy: RetryStrategyProducer =
    RetryStrategy.fibonacciBackOff(initialWaitDuration = 5.seconds, maxAttempts = 3)

  private def getEventClient(force: Boolean = false): ServiceBusSenderClient = {
    if (eventClient == null || force) { // check outside block to avoid single threading
      EventSender.synchronized {
        if (eventClient == null || force) { // check inside block to avoid recreating
          val serviceBusClientBuilder =
            new ServiceBusClientBuilder().connectionString(serviceBusConnectionString)
          eventClient = serviceBusClientBuilder.sender().topicName(eventTopicName).buildClient()
        }
      }
    }

    eventClient
  }

  private def sendMessage(message: EventMessage, safeMessage: Option[String] = None): Unit = {
    Retry(
      // no need to "safe send" this
      getEventClient().sendEventMessage(message)
    ) match {
      case Failure(exception) =>
        logger.warn(
          s"Could not send event message for ${message.sourcePath}. Recreating event client.",
          exception
        )

        // if "safeMessage" is set, then this should only log the error rather than throwing
        safeMessage match {
          case None    => getEventClient(true).sendEventMessage(message)
          case Some(m) => getEventClient(true).safeSendMessage(message, m)
        }
      case Success(_) => // do nothing
    }
  }

  def sendStart(
      subject: String,
      configName: String,
      ingestId: String,
      correlationId: String
  ): Unit = {
    sendMessage(StartMessage(subject, configName, ingestId, correlationId))
  }

  def sendSuccess(
      subject: String,
      configName: String,
      ingestId: String,
      correlationId: String,
      snapshotDate: String,
      ingestedDetails: Seq[WriteInfo]
  ): Unit = {
    sendMessage(
      SuccessMessage(subject, configName, ingestId, correlationId, snapshotDate, ingestedDetails),
      Some(s"$ingestId: Could not send success event")
    )
  }

  def sendFailure(
      subject: String,
      configName: String,
      ingestId: String,
      correlationId: String
  ): Unit = {
    sendMessage(
      FailMessage(subject, configName, ingestId, correlationId),
      Some(s"$ingestId: Could not send failure event")
    )
  }

}

object EventSender extends Logging {

  def apply(serviceBusConnectionString: String, eventTopicName: String): EventSender =
    new EventSender(serviceBusConnectionString, eventTopicName)

  implicit val formats: Formats =
    DefaultFormats + new EnumNameSerializer(MessageStatus)

  implicit class NiceSenderClient(client: ServiceBusSenderClient) {
    def sendEventMessage(eventMessage: EventMessage): Unit = {
      val json = Serialization.write(eventMessage)

      val sm = new ServiceBusMessage(json)
      sm.setSubject(eventMessage.sourcePath)

      client.sendMessage(sm)
    }

    def safeSendMessage(eventMessage: EventMessage, failMessage: String): Unit = {
      try {
        client.sendEventMessage(eventMessage)
      } catch {
        case e: Exception => logger.error(s"${RawLoaderJobUtils.NO_TEAMS_PREFIX}: $failMessage", e)
      }
    }
  }
}
