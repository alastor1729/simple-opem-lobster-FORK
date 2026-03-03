package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.components.SourceConfig.FlattenTarget
import com.bhp.dp.visiting.utils.{DataframeMetadata, SourceData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

case class JsonSource(
    sourceSystem: String,
    multiLine: Boolean,
    allowUnquotedFieldNames: Boolean,
    allowSingleQuotes: Boolean,
    flatten: Option[Boolean] = None,
    primitivesAsString: Option[Boolean] = None,
    dateFormat: Option[String] = None,
    timestampFormat: Option[String] = None,
    flattenTargets: Option[Seq[FlattenTarget]] = None,
    schema: Option[StructType] = None
) extends SourceConfig {

  import SourceConfig._

  private val realFlattenTargets = flattenTargets.getOrElse(Seq.empty)

  override def sourceType: String = "json"

  override def configurationSummary: String =
    s"System=$sourceSystem, Multiline=$multiLine, AllowUnquotedFieldNames=$allowUnquotedFieldNames, AllowSingleQuotes=$allowSingleQuotes, Flatten=$flatten, DateFormat=$dateFormat, TimestampFormat=$timestampFormat"

  private def generateTokens(filePath: String): Map[String, String] = {
    Map(
      "sourceLocation" -> filePath,
      "sourceOffset"   -> ""
    )
  }

  override def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]] = Seq(
    generateTokens(filePath)
  )

  override def getSourceData(
      sparkSession: SparkSession,
      filePath: String,
      ingestId: String
  ): SourceData = {

    var commonDf = sparkSession.read
      .format("json")
      .option("multiline", multiLine)
      // Inferring, even with full sampling, is often inconsistent (thing1 and thing2 are obviously both int, but not when inferred).
      // Ingest should only ever produce human readable and easily normalizable string types.
      .option("dropFieldIfAllNull", value = false) // Preserve entire schema.
      .option("allowUnquotedFieldNames", allowUnquotedFieldNames)
      .option("allowSingleQuotes", allowSingleQuotes)

    commonDf = dateFormat match {
      case Some(c) => commonDf.option("dateFormat", c)
      case None    => commonDf
    }

    commonDf = timestampFormat match {
      case Some(c) => commonDf.option("timestampFormat", c)
      case None    => commonDf
    }

    commonDf = schema match {
      case Some(s) => commonDf.schema(s)
      case None    => commonDf
    }

    // Make primitives as string by default, otherwise use config.
    commonDf = commonDf.option("primitivesAsString", primitivesAsString.getOrElse(true))

    val data = commonDf.load(filePath)

    val explodedData =
      if (realFlattenTargets.nonEmpty) flattenData(data, realFlattenTargets) else data

    val flattenedData = if (flatten.getOrElse(false)) flattenData(explodedData) else explodedData

    val postProcessedData = flattenedData

    SourceData(DataframeMetadata(postProcessedData, generateTokens(filePath)))
  }

}
