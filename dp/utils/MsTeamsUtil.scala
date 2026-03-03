package com.bhp.dp.utils

import com.bhp.dp.events.DataZone.INFRA
import com.bhp.dp.visiting.components.WriteInfo
import com.bhp.dp.visiting.configurations.LoadDefinition
import com.bhp.dp.MsTeamsInfo
import org.apache.commons.lang.{StringUtils => ApacheStringUtils}
import org.apache.logging.log4j.scala.Logging
import sttp.client3._
import sttp.model.Uri

class MsTeamsUtil(
    name: String,
    databricksBaseUrl: String,
    jobId: String
) extends Logging {
  private val jobUrl: Uri = uri"$databricksBaseUrl#job/$jobId/runs"

  def sendSuccess(
      sourcePath: String,
      writeInfos: Seq[WriteInfo],
      load: LoadDefinition,
      correlationId: String,
      ingestId: String,
      isManual: Boolean
  ): Unit = {
    val dataZone   = load.dataZone.getOrElse(INFRA)
    val configName = load.configName.getOrElse("UNKNOWN")

    val facts: List[MSFact] = List(
      MSFact("Team", dataZone.name),
      MSFact("Correlation ID", correlationId),
      MSFact("Ingest ID", ingestId),
      MSFact("Config", configName)
    ) ++
      (if (isManual) List(MSFact("Manual Ingest", "true")) else List.empty) ++
      (if (load.id.nonEmpty) List(MSFact("Config ID", load.id.get)) else List.empty)

    val links: List[MSLink] = List(
      MSLink("Job Url", uri"$jobUrl")
    ) ++
      (if (load.id.nonEmpty)
         List(
           MSLink(
             "Config Url",
             uri"https://datalakeservices.neuehealth.com/configs/${load.id.get}"
           )
         )
       else List.empty)

    val hiveTables = writeInfos
      .map(writeInfo => {
        if (writeInfo.details.isEmpty) {
          // not hive or delta
          writeInfo.ingestedPath
        } else {
          val details = writeInfo.details.get

          if (!details.contains("hiveDatabase")) {
            // not hive or delta
            writeInfo.ingestedPath
          } else if (!details.contains("hiveViewName")) {
            // delta
            s"${details("hiveDatabase")}.${details("hiveTableName")}"
          } else {
            // hive
            s"${details("hiveDatabase")}.${details("hiveViewName")}"
          }
        }
      })
      .distinct
      .foldLeft(new StringBuilder()) { case (builder, details) =>
        builder
          .append(" - ")
          .append(details)
          .append("\n")
      }
      .mkString

    val details: String =
      f"""
         |Completed ingest of $sourcePath to:
         |$hiveTables
         |
         |""".stripMargin

    val card = MsTeamsAlertCard(
      title = f"Raw Loader Success ($name)",
      description = "Completed file ingestion successfully.",
      image =
        "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3f/Commons-emblem-success.svg/32px-Commons-emblem-success.svg.png",
      facts = facts,
      links = links,
      detailsTitle = "Details",
      details = details
    )

    sendMessage(card, dataZone.msTeamsWebhook, null)
  }

  def sendGenericError(exception: Throwable, additionalInfo: String): Unit = {
    val truncatedError = ApacheStringUtils.abbreviate(exception.getMessage, 250).trim

    val facts: List[MSFact] = List(
      MSFact("Team", INFRA.name)
    )

    val links: List[MSLink] = List(
      MSLink("Job Url", uri"$jobUrl")
    )

    val details: String =
      f"""
         |Subject:
         |Non-ingest related exception.
         |
         |Additional Info:
         |$additionalInfo
         |
         |Error:
         |$truncatedError
         |""".stripMargin

    val card = MsTeamsAlertCard(
      title = f"Raw Loader Alert ($name)",
      description = "An error has occurred in rawloader.",
      facts = facts,
      links = links,
      detailsTitle = "Details",
      details = details
    )

    sendMessage(card, INFRA.msTeamsWebhook, exception)
  }

  def sendLoadError(loadException: LoadException, isManual: Boolean = false): Unit = {
    sendLoadError(
      load = loadException.loadDefinition,
      exception = loadException.getCause,
      subject = loadException.subject,
      correlationId = loadException.correlationId,
      ingestId = loadException.ingestId,
      isManual
    )
  }

  def sendLoadError(
      load: LoadDefinition,
      exception: Throwable,
      subject: String,
      correlationId: String,
      ingestId: String,
      isManual: Boolean
  ): Unit = {

    val dataZone   = load.dataZone.getOrElse(INFRA)
    val configName = load.configName.getOrElse("UNKNOWN")

    val truncatedError = ApacheStringUtils.abbreviate(exception.getMessage, 250).trim

    val facts: List[MSFact] = List(
      MSFact("Team", dataZone.name),
      MSFact("Correlation ID", correlationId),
      MSFact("Ingest ID", ingestId),
      MSFact("Config", configName)
    ) ++
      (if (isManual) List(MSFact("Manual Ingest", "true")) else List.empty) ++
      (if (load.id.nonEmpty) List(MSFact("Config ID", load.id.get)) else List.empty)

    val links: List[MSLink] = List(
      MSLink("Job Url", uri"$jobUrl")
    ) ++
      (if (load.id.nonEmpty)
         List(
           MSLink(
             "Config Url",
             uri"https://datalakeservices.neuehealth.com/configs/${load.id.get}"
           )
         )
       else List.empty)

    val details: String =
      f"""
         |Subject:
         |Failed to ingest source from `$subject`
         |
         |Error:
         |$truncatedError
         |""".stripMargin

    val card = MsTeamsAlertCard(
      title = f"Raw Loader Alert ($name)",
      description = "An error has occurred in rawloader.",
      facts = facts,
      links = links,
      detailsTitle = "Details",
      details = details
    )

    sendMessage(card, dataZone.msTeamsWebhook, exception)
  }

  private def sendMessage(
      card: MsTeamsAlertCard,
      msHookUrl: Uri,
      exception: Throwable
  ): Unit = {

    try {
      MsTeamsUtils.post(
        hookUrl = msHookUrl,
        card = card
      )
    } catch {
      case ex: Exception =>
        ex.addSuppressed(exception)
        logger.error(s"${RawLoaderJobUtils.NO_TEAMS_PREFIX} Sending Teams message failed", ex)
    }
  }
}

object MsTeamsUtil {
  def apply(
      name: String,
      databricksBaseUrl: String,
      jobId: String
  ): MsTeamsUtil = new MsTeamsUtil(
    name,
    databricksBaseUrl,
    jobId
  )

  def apply(msTeamsInfo: MsTeamsInfo): MsTeamsUtil = new MsTeamsUtil(
    msTeamsInfo.name,
    msTeamsInfo.databricksBaseUrl,
    msTeamsInfo.jobId
  )
}
