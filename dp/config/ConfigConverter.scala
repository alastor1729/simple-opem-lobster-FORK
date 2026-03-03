package com.bhp.dp.config

import com.bhp.dp.events.DataZoneSerializer
import com.bhp.dp.visiting.components.{SourceConfig, TargetConfig}
import com.bhp.dp.visiting.configurations._
import com.bhp.dp.visiting.utils.{SaveModeEnumSerializer, StructTypeSerializer}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read
import org.reflections.Reflections

import scala.collection.JavaConverters._

object ConfigConverter {

  private val refls = new Reflections("com.bhp.dp.visiting.configurations")
  private val sourceConfigClasses = refls
    .getSubTypesOf(classOf[SourceConfig])
    .iterator()
    .asScala
    .filter(_.getName.startsWith("com.bhp.dp.visiting.configurations"))
    .toList
  private val targetConfigClasses = refls
    .getSubTypesOf(classOf[TargetConfig])
    .iterator()
    .asScala
    .filter(_.getName.startsWith("com.bhp.dp.visiting.configurations"))
    .toList

  // This implicit is configuring the classes/types for liftweb-json to parse to.
  implicit val formats: Formats = Serialization.formats(
    ShortTypeHints(sourceConfigClasses ++ targetConfigClasses)
  ) + new SaveModeEnumSerializer() + new StructTypeSerializer() + new DataZoneSerializer()

  def convertFullJsonConfig(json: String): LoadDefinition = {
    val config = read[LoadDefinition](json)

    val accountName = config.storageAccount.getOrElse("unknown_account")
    val container   = config.containerName.getOrElse("unknown_container")
    val path        = config.path.getOrElse("unknown_path")

    config.copy(configName = Some(s"$accountName.$container -> $path"))
  }

  def convertJsonConfig(
      json: String,
      parameters: ConfigParameters,
      configName: String,
      containerName: String,
      storageAccount: String
  ): LoadDefinition = {
    val loadDefinitionJson = findReplaceParameters(json, parameters)

    try {
      val configParts = configName.split("@")

      convertFullJsonConfig(loadDefinitionJson)
        .copy(
          configName = Some(configName),
          path = Some(configParts.dropRight(1).mkString("/")),
          extension = Some(configParts.last.replace(".json", "")),
          containerName = Some(containerName),
          storageAccount = Some(storageAccount)
        )
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed loading $configName: ${e.getMessage}", e)
    }
  }

  private def findReplaceParameters(json: String, parameters: ConfigParameters): String = {
    var replacedJson = json
    parameters.params.foreach(p =>
      replacedJson = replacedJson.replaceAll("\\$\\{" + p._1 + "}", p._2)
    )

    replacedJson
  }
}
