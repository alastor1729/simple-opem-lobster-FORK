package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.{DataframeMetadata, SchemaMutations, SourceData}
import org.apache.spark.sql.SparkSession

case class ParquetSource(sourceSystem: String, headerColumns: Option[Seq[String]] = None)
    extends SourceConfig {

  headerColumns.foreach(validateColumns)

  override def sourceType: String = "parquet"

  override def configurationSummary: String = s"System=$sourceSystem"

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
    val df = sparkSession.read.parquet(filePath)

    val renamedDf = headerColumns match {
      case Some(newNames) => SchemaMutations.rename(df, newNames)
      case None           => df
    }

    SourceData(DataframeMetadata(renamedDf, generateTokens(filePath)))
  }
}
