package com.bhp.dp


import com.bhp.dp.events.EventSender
import com.bhp.dp.utils.{JobArgs, RawLoaderJobUtils}
import com.bhp.dp.RawLoaderArgs.{getDatalakeInfo, getFileMoverInfo, getInboundServiceBusInfo, getMsTeamsInfo}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.util.Try


class RawLoaderArgs(args: Array[String]) extends JobArgs(args = args) {

  val configPath: String = getOptionalStringValue("-config").getOrElse(
    throw new IllegalArgumentException("Must specify config")
  )

  val myConfig: Config = ConfigFactory.load(configPath).getConfig("rawLoader")

  // Args
  val ingestConfigName: Option[String] = getOptionalStringValue("-ingestConfig")
  val rawDataPaths: Seq[String]        = getCliCommaSeparatedString("-files", Seq.empty)

  val isReingest: Boolean       = args.contains("-reingest")
  val manualSendEvents: Boolean = args.contains("-sendEvents")

  val ingestThreads: Int = getOptionalStringValue("-ingestThreads")
    .map(_.toInt)
    .getOrElse(Try(myConfig.getInt("ingestThreads")).getOrElse(10))

  val msTeamsInfo: Option[MsTeamsInfo] =
    if (myConfig.hasPath("msTeamsInfo")) Some(getMsTeamsInfo(myConfig.getConfig("msTeamsInfo")))
    else None

  lazy val inboundServiceBusInfo: InboundServiceBusInfo = getInboundServiceBusInfo(
    myConfig.getConfig("inboundServiceBusInfo")
  )

  lazy val datalakeInfos: Seq[DatalakeInfo] =
    myConfig.getConfigList("datalakes").asScala.map(getDatalakeInfo)

  if (datalakeInfos.isEmpty)
    throw new IllegalArgumentException(
      s"Configuration $configPath must have at least one datalake specified."
    )

  lazy val fileMoverInfo: FileMoverInfo = getFileMoverInfo(
    myConfig.getConfig("fileMoverInfo")
  )
}

case class MsTeamsInfo(
    name: String,
    databricksBaseUrl: String,
    jobId: String
)

case class InboundServiceBusInfo(
    connectionString: String,
    topicName: String,
    subscriptionName: String,
    cancelTopicName: String,
    cancelSubscriptionName: String
)

case class FileMoverInfo(
    url: String,
    secret: String
)

case class OutboundServiceBusInfo(connectionString: String, topicName: String) {
  lazy val eventSender: EventSender = EventSender(connectionString, topicName)
}

case class AccountInfo(name: String, key: String)


trait DatalakeInfo {
  val environment: String
  val lineOfBusiness: Option[String]
  val accountInfo: Option[AccountInfo]
  val outboundServiceBusInfos: Seq[OutboundServiceBusInfo]
  val connectionString: Option[String]
  val eventSenders: Seq[EventSender]
}

case class DatalakeInfoImpl(
    environment: String,
    lineOfBusiness: Option[String],
    accountInfo: Option[AccountInfo],
    outboundServiceBusInfos: Seq[OutboundServiceBusInfo]
) extends DatalakeInfo {

  val connectionString: Option[String] =
    accountInfo.map(
      ai => s"DefaultEndpointsProtocol=https;AccountName=${ai.name};AccountKey=${ai.key};EndpointSuffix=core.windows.net"
  )

  val eventSenders: Seq[EventSender] = outboundServiceBusInfos.map(_.eventSender)
}

object RawLoaderArgs {

  private def getMsTeamsInfo(msTeamsConfig: Config): MsTeamsInfo = {
    val name = msTeamsConfig.getString("name")

    val databricksBaseUrl = msTeamsConfig.getString("databricksBaseUrl")
    val jobId             = msTeamsConfig.getString("jobId")

    MsTeamsInfo(
      name,
      databricksBaseUrl,
      jobId
    )
  }

  private def getInboundServiceBusInfo(inboundConfig: Config): InboundServiceBusInfo = {
    val connectionString       = getFromSecret(inboundConfig.getString("connectionStringSecret"))
    val topicName              = inboundConfig.getString("topicName")
    val subscriptionName       = inboundConfig.getString("subscriptionName")
    val cancelTopicName        = inboundConfig.getString("cancelTopicName")
    val cancelSubscriptionName = inboundConfig.getString("cancelSubscriptionName")

    InboundServiceBusInfo(
      connectionString,
      topicName,
      subscriptionName,
      cancelTopicName,
      cancelSubscriptionName
    )
  }

  private def getFileMoverInfo(fileMoverConfig: Config): FileMoverInfo = {
    FileMoverInfo(
      fileMoverConfig.getString("url"),
      getFromSecret(fileMoverConfig.getString("secret"))
    )
  }

  private def getDatalakeInfo(config: Config): DatalakeInfo = {
    val environment    = config.getString("environment")
    val lineOfBusiness = Try(config.getString("lineOfBusiness")).toOption

    val accountInfo = if (config.hasPath("accountInfo")) {
      val accountInfoConfig = config.getConfig("accountInfo")

      Some(
        AccountInfo(
          name = accountInfoConfig.getString("name"),
          key = getFromSecret(accountInfoConfig.getString("keySecret"))
        )
      )
    } else {
      None
    }

    val outboundServiceBusInfos = if (config.hasPath("outboundServiceBusInfos")) {
      config
        .getConfigList("outboundServiceBusInfos")
        .asScala
        .map(c => {
          OutboundServiceBusInfo(
            connectionString = getFromSecret(c.getString("connectionStringSecret")),
            topicName = c.getString("topicName")
          )
        })
    } else {
      Seq.empty
    }

    DatalakeInfoImpl(
      environment,
      lineOfBusiness,
      accountInfo,
      outboundServiceBusInfos
    )
  }


  private def getFromSecret(secretInfo: String): String = {
    if (RawLoaderJobUtils.isLocal) {
      secretInfo
    } else {
      val split = secretInfo.split("\\.")

      if (split.size != 2)
        throw new IllegalArgumentException(
          s"Could not locate secret scope and key from $secretInfo"
        )

      val scope = split(0)
      val key   = split(1)

      dbutils.secrets.get(scope, key)
    }
  }

}
