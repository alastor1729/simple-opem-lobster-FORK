package com.bhp.dp.config

import com.bhp.dp.{DatalakeInfo, FileMoverInfo}
import com.bhp.dp.utils.{FileMoverUtils, MsTeamsUtil}
import com.bhp.dp.visiting.configurations.LoadDefinition
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.commons.io.FilenameUtils
import org.apache.logging.log4j.scala.Logging


import java.io.File
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Using


trait IConfigLoader {
  def getConfig(
      datalakeInfo: DatalakeInfo,
      container: String,
      subject: String
  ): Option[LoadDefinition]
}


class LocalConfigLoader(overrideConfigPath: String) extends IConfigLoader with Logging {

  logger.warn(s"\n\n!!! Override config set at path $overrideConfigPath !!!\n\n")

  def getConfig(
      datalakeInfo: DatalakeInfo,
      container: String,
      subject: String
  ): Option[LoadDefinition] = {
    val configParameters = ConfigParameters(datalakeInfo.environment, datalakeInfo.lineOfBusiness)
    val file             = new File(overrideConfigPath)

    val contents = Using(Source.fromFile(file))(_.mkString).get

    Some(
      ConfigConverter.convertJsonConfig(
        contents,
        configParameters,
        file.getName,
        container,
        datalakeInfo.accountInfo.map(_.name).getOrElse("local")
      )
    )
  }
}

object LocalConfigLoader {
  def apply(overrideConfigPath: String): LocalConfigLoader = new LocalConfigLoader(
    overrideConfigPath
  )
}

class ConfigLoader(fileMoverInfo: FileMoverInfo, teamsUtil: MsTeamsUtil)
    extends IConfigLoader
    with Logging {
  import ConfigLoader._

  // this will return a quick 204 on creation to validate that the connection works at startup
  // if this fails, it will cause the RawLoader job to fail before it starts to process messages
  FileMoverUtils.getConfigBySubject(
    fileMoverInfo,
    "no_account",
    "no_container",
    "no_subject",
    teamsUtil
  )

  def getConfig(
      datalakeInfo: DatalakeInfo,
      container: String,
      subject: String
  ): Option[LoadDefinition] = {
    val accountName = datalakeInfo.accountInfo.map(_.name).getOrElse("unknown")

    val shortSubject = if (subject.contains("/snapshot_date")) {
      subject.substring(0, subject.indexOf("/snapshot_date"))
    } else {
      subject
    }

    val extension = FilenameUtils.getExtension(subject)

    getConfig(CacheKey(accountName, container, shortSubject, extension), subject)
  }

  private def getConfig(key: CacheKey, subject: String): Option[LoadDefinition] = {
    configCache.get(
      key,
      () =>
        FileMoverUtils.getConfigBySubject(
          fileMoverInfo,
          key.accountName,
          key.container,
          subject,
          teamsUtil
        )
    )
  }
}

object ConfigLoader extends Logging {
  def apply(fileMoverInfo: FileMoverInfo, teamsUtil: MsTeamsUtil): ConfigLoader =
    new ConfigLoader(fileMoverInfo, teamsUtil)
  case class CacheKey(
      accountName: String,
      container: String,
      shortSubject: String,
      extension: String
  )

  private val configCache: Cache[CacheKey, Option[LoadDefinition]] = CacheBuilder
    .newBuilder()
    .maximumSize(100)
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .expireAfterAccess(30, TimeUnit.SECONDS)
    .build()
}
