package com.bhp.dp

import com.azure.messaging.servicebus._
import com.bhp.dp.config.{ConfigHelper, ConfigLoader}
import com.bhp.dp.logging.LoggingConstants
import com.bhp.dp.streaming.{HandleFileMessage, HandleMessage, MessageMetadata}
import com.bhp.dp.utils.MsTeamsUtil
import org.apache.logging.log4j.{CloseableThreadContext, ThreadContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Using

class RawLoaderStreamingImpl(
    sparkSession: SparkSession,
    datalakeInfos: Seq[DatalakeInfo],
    configLoader: ConfigLoader, // this does NOT support config overrides so it forces use of ConfigLoader
    inboundServiceBusInfo: InboundServiceBusInfo,
    msTeamsUtil: MsTeamsUtil
) extends Logging {

  import RawLoaderStreamingImpl._

  private val MAX_DURATION_WITHOUT_MESSAGE = Duration.ofMinutes(5)

  datalakeInfos.foreach(dlI =>
    if (dlI.accountInfo.isEmpty)
      throw new IllegalArgumentException(
        s"Datalake Account Info is empty for ${dlI.lineOfBusiness.getOrElse(dlI.environment)}"
      )
  )

  def listen(concurrentMessageCount: Int = 10): Unit = {

    val serviceBusConnectionString = inboundServiceBusInfo.connectionString
    val topicName                  = inboundServiceBusInfo.topicName
    val subscriptionName           = inboundServiceBusInfo.subscriptionName
    val cancelTopicName            = inboundServiceBusInfo.cancelTopicName
    val cancelSubscriptionName     = inboundServiceBusInfo.cancelSubscriptionName

    logger.info(
      s"Listening to $topicName/$subscriptionName and $cancelTopicName/$cancelSubscriptionName with $concurrentMessageCount concurrent messages."
    )

    val hasBeenCancelled = new AtomicBoolean(false)
    import scala.concurrent.ExecutionContext.Implicits.global

    val messageLockExecutionContext =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(concurrentMessageCount * 2))

    val topLevelContextValues = ThreadContext.getImmutableContext

    def buildSubscriberFuture(): Future[HandleFileMessage] = {
      val serviceBusClientBuilder =
        new ServiceBusClientBuilder().connectionString(serviceBusConnectionString)

      logger.info("Building new client and subscriber.")
      val receiverClientBuilder =
        getBuilder(serviceBusClientBuilder, topicName, subscriptionName)
      val client       = receiverClientBuilder.buildClient()
      val senderClient = serviceBusClientBuilder.sender().topicName(topicName).buildClient()

      val subscriber = new HandleFileMessage(
        hasBeenCancelled,
        client,
        senderClient,
        messageLockExecutionContext,
        ingestCallback,
        datalakeInfos,
        msTeamsUtil,
        concurrentMessageCount,
        MAX_DURATION_WITHOUT_MESSAGE
      )

      Future {
        Using(CloseableThreadContext.putAll(topLevelContextValues)) { _ =>
          {
            subscriber.run()
            subscriber
          }
        }.get
      } recoverWith { case throwable: Throwable =>
        Using(CloseableThreadContext.putAll(topLevelContextValues)) { _ =>
          {
            logger.error(
              "Error in HandleFileMessage. May indicate connection issues. Will retry.",
              throwable
            )
            msTeamsUtil.sendGenericError(
              throwable,
              "Error in HandleFileMessage. May indicate connection issues. Will retry."
            )
            closeHandler(subscriber, "file subscriber")
            logger.warn("Retrying file subscriber.")
          }
        }

        buildSubscriberFuture()
      }
    }

    val subscriberFuture: Future[HandleFileMessage] = buildSubscriberFuture()

    val subscribeWithClose = subscriberFuture.map(s => {
      logger.warn(s"Closing file subscriber")
      closeHandler(s, "file subscriber")
    })

    Await.ready(
      subscribeWithClose,
      concurrent.duration.Duration.Inf
    )

    logger.info("All subscriptions and receivers closed. Exiting.")
  }

  private def closeHandler(handler: HandleMessage, name: String): Unit = {
    logger.warn(s"Waiting for any existing work for $name.")
    handler.waitForFinalWork()
    logger.warn(s"Closing $name clients.")
    handler.client.close()
    handler.senderClient.close()
  }

  private def ingestCallback(
      datalakeInfo: DatalakeInfo,
      trimmedSubject: String,
      container: String,
      correlationId: String,
      metadata: MessageMetadata,
      ctx: CloseableThreadContext.Instance
  ): Unit = {
    val lobMountString = ConfigHelper.getLobMountString(datalakeInfo.lineOfBusiness)
    val mountedSubject =
      s"abfss://$container@neudatalakeprsa01.dfs.core.windows.net/$trimmedSubject"
    // replace MDC item with more familiar mounted subject
    ctx.put(LoggingConstants.FILE_PATH, mountedSubject)

    val loadDefinition = configLoader.getConfig(datalakeInfo, container, trimmedSubject)

    loadDefinition match {
      case Some(load) =>
        val configName = load.configName.getOrElse("UNKNOWN")

        logger.info(
          s"Running WriteSourceToTarget using $configName from ${load.source.configurationSummary} to ${load.target.configurationSummary} for $mountedSubject"
        )

        val fileContextValues: util.Map[String, String] = ThreadContext.getImmutableContext
        val writeInfos = RawLoaderJobImpl.WriteSourceToTarget(
          sparkSession,
          load,
          mountedSubject,
          metadata.messageId,
          correlationId,
          datalakeInfo.eventSenders,
          fileContextValues
        )

        if (load.notifyOnSuccess.getOrElse(false)) {
          msTeamsUtil.sendSuccess(
            sourcePath = mountedSubject,
            writeInfos = writeInfos,
            load = load,
            correlationId = correlationId,
            ingestId = metadata.messageId,
            isManual = false
          )
        }

        logger.info(s"Finished load for $mountedSubject")
      case None =>
        logger.info(
          s"Could not locate config for subject $trimmedSubject in ${datalakeInfo.accountInfo.map(_.name).getOrElse("unknown")}.$container. Skipping."
        )
    }
  }
}

object RawLoaderStreamingImpl extends Logging {
  private def getBuilder(
      serviceBusClientBuilder: ServiceBusClientBuilder,
      topic: String,
      subscription: String
  ): ServiceBusClientBuilder#ServiceBusReceiverClientBuilder = {
    serviceBusClientBuilder
      .receiver()
      .topicName(topic)
      .subscriptionName(subscription)
      .maxAutoLockRenewDuration(Duration.ZERO)
      .disableAutoComplete()
      .prefetchCount(0)
  }

  def apply(
      sparkSession: SparkSession,
      datalakeInfos: Seq[DatalakeInfo],
      configLoader: ConfigLoader,
      inboundServiceBusInfo: InboundServiceBusInfo,
      msTeamsUtil: MsTeamsUtil
  ): RawLoaderStreamingImpl = {
    new RawLoaderStreamingImpl(
      sparkSession,
      datalakeInfos,
      configLoader,
      inboundServiceBusInfo,
      msTeamsUtil
    )
  }
}
