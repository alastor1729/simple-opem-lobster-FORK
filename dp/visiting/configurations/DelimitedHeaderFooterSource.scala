package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.components.SourceConfig.SOURCE_LINE_NUMBER_COL_NAME
import com.bhp.dp.visiting.utils.{DataframeMetadata, SchemaMutations, SourceData}
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class RecordType(typeName: String, recordValue: String, columns: Seq[String])

case class DelimitedHeaderFooterSource(
    sourceSystem: String,
    delimiter: String,
    recordTypeIndex: Int,
    encoding: Option[String] = None,
    multiLine: Option[Boolean] = None,
    types: Seq[RecordType]
) extends SourceConfig {

  if (types.isEmpty)
    throw new IllegalArgumentException(
      "DelimitedHeaderFooterSource.types is required to not be empty"
    )

  if (types.exists(_.columns.isEmpty))
    throw new IllegalArgumentException(
      "DelimitedHeaderFooterSource.types.columns is required to not be empty"
    )

  types.foreach(r => validateColumns(r.columns))

  override def sourceType: String = "delimitedHeaderFooter"

  override def configurationSummary: String =
    s"System=$sourceSystem, Encoding=$encoding, MultiLine=$multiLine, Types=[${types
      .map(t => s"(${t.typeName}, ${t.columns.size} cols)")
      .mkString(", ")}]"

  private def generateTokens(filePath: String, recordType: RecordType): Map[String, String] = {
    Map(
      "sourceLocation" -> filePath,
      "sourceOffset"   -> "",
      "type"           -> recordType.typeName
    )
  }

  override def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]] =
    types.map(t => generateTokens(filePath, t))

  override def getSourceData(
      sparkSession: SparkSession,
      filePath: String,
      ingestId: String
  ): SourceData = {
    import sparkSession.implicits._
    val optionedReader: DataFrameReader = sparkSession.read
      .option("ignoreLeadingWhiteSpace", value = true)  // you need this
      .option("ignoreTrailingWhiteSpace", value = true) // and this
      .option("delimiter", delimiter)
      .option("inferSchema", value = false)
      .option("encoding", encoding.getOrElse("UTF-8"))
      .option("multiLine", multiLine.getOrElse(false))

    val numCols = types.map(_.columns.size).max
    val schema  = StructType((1 to numCols).map(n => StructField(s"col$n", StringType, true)))

    val numberedDF = CsvSource.getLineNumberedDF(
      sparkSession,
      optionedReader,
      filePath,
      multiLine.getOrElse(false),
      schema = Some(schema)
    )

    SourceData(
      types.map(t => {
        val columnCount   = t.columns.size
        val colsToSelect  = (1 to columnCount).map(n => $"col$n") :+ $"$SOURCE_LINE_NUMBER_COL_NAME"
        val recordTypeCol = $"col$recordTypeIndex"

        val df = numberedDF.select(colsToSelect: _*).where(trim(recordTypeCol).rlike(t.recordValue))

        val finalDF = SchemaMutations.rename(df, t.columns :+ SOURCE_LINE_NUMBER_COL_NAME)
        DataframeMetadata(finalDF, generateTokens(filePath, t))
      })
    )
  }

}
