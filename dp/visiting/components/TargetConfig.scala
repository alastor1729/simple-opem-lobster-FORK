package com.bhp.dp.visiting.components

import com.bhp.dp.visiting.utils.DataframeMetadata
import org.apache.spark.sql.SparkSession

abstract class TargetConfig extends Config {

  def targetType: String

  protected def additionalTokens: Map[String, String] = Map.empty

  override def tokens: Map[String, String] = {
    Map(
      "targetType" -> targetType
    ) ++ additionalTokens
  }

  def deleteData(
      sparkSession: SparkSession,
      deleteTokens: Map[String, String],
      metaFileName: Option[String] = None
  ): Unit

  def writeData(df: DataframeMetadata, ingestId: String): WriteInfo
}

object TargetConfig {
  def replaceTokens(source: String, tokens: Map[String, String]): String = {
    tokens.foldLeft(source)((acc, cur) => {
      acc.replace("{" + cur._1 + "}", cur._2)
    })
  }
}

case class WriteInfo(ingestedPath: String, details: Option[Map[String, String]] = None)
