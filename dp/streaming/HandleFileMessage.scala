package com.bhp.dp.streaming

import com.azure.messaging.servicebus.{ServiceBusReceiverClient, ServiceBusSenderClient}
import com.azure.storage.blob.models.{BlobProperties, CopyStatusType}
import com.azure.storage.blob.BlobClientBuilder
import com.bhp.dp.config.ConfigHelper
import com.bhp.dp.logging.LoggingConstants
import com.bhp.dp.utils.{FileMoverException, LoadException, MsTeamsUtil}
import com.bhp.dp.DatalakeInfo
import org.apache.commons.lang.time.DurationFormatUtils
import org.apache.logging.log4j.CloseableThreadContext

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** Handles the File Ingest messages based on the message contents matching to the DatalakeInfos
  *
  * @param hasBeenCancelled
  *   AtomicBoolean as to whether the job has been cancelled
  * @param client
  *   The ServiceBusReceiverClient. Used to Complete, Abandon, or Deadletter messages
  * @param senderClient
  *   The ServiceBusSenderClient. Used to send Delayed messages
  * @param messageLockExecutionContext
  *   ExecutionContext to hold threads to lock messages
  * @param workOnSubject
  *   Function to work on the subject
  * @param datalakeInfos
  *   Seq of DatalakeInfos to ingest files into
  * @param msTeamsUtil
  *   MsTeamsUtil used to send error messages to teams
  * @param concurrentMessageCount
  *   Max number of messages being processed at once
  * @param maxLastMessageDuration
  *   Limits the amount of time this will listen without getting a message. Will warn and end if the
  *   time limit is exceeded.
  */
class HandleFileMessage(
    hasBeenCancelled: AtomicBoolean,
    client: ServiceBusReceiverClient,
    senderClient: ServiceBusSenderClient,
    messageLockExecutionContext: ExecutionContext,
    workOnSubject: (
        DatalakeInfo,
        String,
        String,
        String,
        MessageMetadata,
        CloseableThreadContext.Instance
    ) => Unit,
    datalakeInfos: Seq[DatalakeInfo],
    msTeamsUtil: MsTeamsUtil,
    concurrentMessageCount: Int,
    maxLastMessageDuration: Duration
) extends HandleMessage(
      hasBeenCancelled,
      client,
      senderClient,
      messageLockExecutionContext,
      msTeamsUtil,
      concurrentMessageCount,
      Some(maxLastMessageDuration)
    ) {

  import HandleFileMessage._

  override def handleEventGridMessage(
      message: EventGridMessage,
      metadata: MessageMetadata,
      ctx: CloseableThreadContext.Instance
  ): Outcome = {
    val subject = message.subject
    ctx.put(LoggingConstants.FILE_PATH, subject)

    lazy val container      = ConfigHelper.getContainerBlobbed(subject)
    lazy val trimmedSubject = ConfigHelper.cleanBlobSubject(subject)
    lazy val datalakeInfoOpt = datalakeInfos.find(dlI =>
      message.topic.toLowerCase.contains(dlI.accountInfo.get.name.toLowerCase)
    )

    try {
      datalakeInfoOpt match {
        case Some(datalakeInfo) =>
          message.eventType match {
            case "Microsoft.Storage.BlobCreated" =>
              message.data.api match {
                case "CreateFile" =>
                  logger.info(s"Skipping CreateFile API")
                  Outcome.Complete

                case "CopyBlob" =>
                  // special handling for CopyBlob since it is sent at the START of the copy, not the end.
                  // check the copy status and delay if not complete or failed/aborted

                  container match {
                    case Some(c) =>
                      val properties =
                        getBlobProperties(datalakeInfo.connectionString.get, trimmedSubject, c)

                      val correlationId = properties
                        .flatMap(p => getCorrelationId(p.getMetadata))
                        .getOrElse(metadata.messageId)

                      ctx.put(LoggingConstants.CORRELATION_ID, correlationId)

                      logger.info(
                        s"Message is a CopyBlob operation. Checking copy status."
                      )

                      val copyStatus = properties.map(_.getCopyStatus).orNull

                      copyStatus match {
                        case CopyStatusType.PENDING =>
                          // copy is in progress
                          logger.info(
                            s"Copy status for $subject is $copyStatus. Waiting $COPY_WAIT_DURATION_ENGLISH."
                          )
                          Outcome.Delay(COPY_WAIT_DURATION)
                        case CopyStatusType.SUCCESS =>
                          // process the file
                          logger.info(
                            s"Copy status for $subject is $copyStatus. Processing."
                          )
                          workOnSubject(
                            datalakeInfo,
                            trimmedSubject,
                            c,
                            correlationId,
                            metadata,
                            ctx
                          )
                          Outcome.Complete
                        case CopyStatusType.ABORTED | CopyStatusType.FAILED =>
                          // log aborted/failed as warning. complete with no error
                          logger.warn(
                            s"Copy status for $subject is $copyStatus. Skipping."
                          )
                          Outcome.Complete
                        case null =>
                          // null copy status indicates that either the blob was not the target of a copy operation or was modified since the copy operation started
                          logger.warn(
                            s"Copy status for $subject is $copyStatus. Treating as 'success'."
                          )
                          workOnSubject(
                            datalakeInfo,
                            trimmedSubject,
                            c,
                            correlationId,
                            metadata,
                            ctx
                          )
                          Outcome.Complete
                      }
                    case None =>
                      logger.warn(
                        s"Unable to determine the source container given subject $subject"
                      )
                      Outcome.Complete
                  }

                case _ =>
                  // do the ingestion
                  container match {
                    case Some(c) =>
                      val properties =
                        getBlobProperties(datalakeInfo.connectionString.get, trimmedSubject, c)

                      val correlationId = properties
                        .flatMap(p => getCorrelationId(p.getMetadata))
                        .getOrElse(metadata.messageId)
                      ctx.put(LoggingConstants.CORRELATION_ID, correlationId)

                      workOnSubject(datalakeInfo, trimmedSubject, c, correlationId, metadata, ctx)
                    case None =>
                      logger.warn(
                        s"Unable to determine the source container given subject $subject"
                      )
                  }
                  Outcome.Complete
              }

            case _ =>
              logger.warn(s"Unhandled event type ${message.eventType}")
              Outcome.DeadLetter(s"Unhandled event type ${message.eventType}")
          }
        case None =>
          logger.warn(
            s"Unable to determine the datalake given topic ${message.topic}"
          )
          Outcome.Complete
      }
    } catch {
      case e: FileMoverException =>
        if (metadata.delayCount > 3) throw e

        logger.error("Failed pulling config from FileMover. Delaying message 2 minutes.", e)
        Outcome.Delay(Duration.ofMinutes(2))
    }

  }

  override def handleThrowable(t: Throwable): Option[Throwable] = {
    t match {
      case loadException: LoadException =>
        val cause = loadException.getCause
        logger.error("Exception during ingest", cause)
        msTeamsUtil.sendLoadError(loadException)

        Some(cause)
      case _ => None
    }
  }

  private def getBlobProperties(
      blobConnectionString: String,
      trimmedSubject: String,
      container: String
  ): Option[BlobProperties] = {
    val blobClientBuilder = new BlobClientBuilder().connectionString(blobConnectionString)
    val blockClient = blobClientBuilder
      .containerName(container)
      .blobName(trimmedSubject)
      .buildClient()
      .getBlockBlobClient

    val triedProperties = Try(blockClient.getProperties)

    triedProperties match {
      case Failure(exception) => logger.warn("Could not pull blob properties", exception)
      case Success(_)         => // ignore
    }

    triedProperties.toOption
  }

  private def getCorrelationId(metadata: java.util.Map[String, String]): Option[String] = {
    // have to iterate through these because keys are "case insensitive" and may be upper or lower case!
    metadata.asScala
      .find { case (key, _) =>
        key.equalsIgnoreCase(LoggingConstants.CORRELATION_ID)
      }
      .map(_._2)
  }
}

object HandleFileMessage {
  val COPY_WAIT_DURATION: Duration = Duration.ofMinutes(5)
  val COPY_WAIT_DURATION_ENGLISH: String =
    DurationFormatUtils.formatDurationWords(COPY_WAIT_DURATION.toMillis, true, true)
}
