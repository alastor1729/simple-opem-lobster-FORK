package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.components.SourceConfig.SOURCE_LINE_NUMBER_COL_NAME
import com.bhp.dp.visiting.utils.{DataframeMetadata, SchemaMutations, SourceData}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

case class CsvSource(
    sourceSystem: String,
    delimiter: String,
    hasHeader: Boolean,
    encoding: Option[String] = None,
    skipLines: Option[Int] = None,
    headerColumns: Option[Seq[String]] = None,
    multiLine: Option[Boolean] = None,
    escapeChar: Option[String] = None
) extends SourceConfig {

  headerColumns.foreach(validateColumns)

  override def sourceType: String = "csv"

  override def configurationSummary: String =
    s"System=$sourceSystem, Delimiter=$delimiter, HasHeader=$hasHeader, Encoding=$encoding, SkipLines=$skipLines, MultiLine=$multiLine"

  private def generateTokens(filePath: String): Map[String, String] = {
    Map(
      "sourceLocation" -> filePath,
      "sourceOffset"   -> skipLines.map(x => x.toString).getOrElse("")
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
    val optionedReader: DataFrameReader = sparkSession.read
      .option("ignoreLeadingWhiteSpace", value = true)  // you need this
      .option("ignoreTrailingWhiteSpace", value = true) // and this
      .option("delimiter", delimiter)
      .option("header", hasHeader)
      .option("inferSchema", value = false)
      .option("encoding", encoding.getOrElse("UTF-8"))
      .option("multiLine", multiLine.getOrElse(false))
      .option("escape", escapeChar.getOrElse("\\"))

    val numberedDF = CsvSource.getLineNumberedDF(
      sparkSession,
      optionedReader,
      filePath,
      multiLine.getOrElse(false),
      skipLines = skipLines,
      hasHeader = hasHeader
    )

    val renamedDf = headerColumns match {
      case Some(newNames) =>
        SchemaMutations.rename(numberedDF, newNames :+ SOURCE_LINE_NUMBER_COL_NAME)
      case None => numberedDF
    }

    SourceData(DataframeMetadata(renamedDf, generateTokens(filePath)))
  }
}

object CsvSource {
  def getLineNumberedDF(
      sparkSession: SparkSession,
      optionedReader: DataFrameReader,
      filePath: String,
      multiLine: Boolean,
      schema: Option[StructType] = None,
      skipLines: Option[Int] = None,
      hasHeader: Boolean = false
  ): DataFrame = {
    import sparkSession.implicits._

    val originalDF = skipLines match {
      case Some(skipLines) =>
        val rdd = sparkSession.sparkContext.textFile(filePath)
        val rddSkippedLines = rdd.mapPartitionsWithIndex((partitionIndex, r) =>
          if (partitionIndex == 0) r.drop(skipLines) else r
        )
        val ds = sparkSession.createDataset(rddSkippedLines)
        optionedReader.csv(ds)
      case None =>
        schema match {
          case Some(s) => optionedReader.schema(s).csv(filePath)
          case None    => optionedReader.csv(filePath)
        }
    }

    val dfCols          = originalDF.columns.map(col)
    val dfColsAsStrings = originalDF.columns.map(c => coalesce(col(c).cast(StringType), lit("")))
    val dfSchema        = originalDF.schema

    val allStringCol = concat(dfColsAsStrings: _*)

    // 1 for 0 based id, # for skipped lines, 1 if header
    val fullSkipCount = 1 + skipLines.getOrElse(0) + (if (hasHeader) 1 else 0)

    val rdd = originalDF.rdd.zipWithIndex().map { case (row, idx) =>
      Row.fromSeq(row.toSeq :+ (idx + fullSkipCount))
    }

    val logicallyNumberedDF = sparkSession.createDataFrame(
      rdd,
      StructType(dfSchema :+ StructField("logicalNumber", LongType))
    )
    val numberedDF = if (multiLine) {
      // with multiline the logical numbering does not match the true line number, for each line have to calculate the number of previous newlines
      val linesInAString =
        (s: String) =>
          Option(s)
            .map(_.split("\r\n|\r|\n").length)
            .getOrElse(1) // there  is always at least 1 line
      val linesCountUDF = udf(linesInAString)

      val withLinesCount =
        logicallyNumberedDF.withColumn("newlines", linesCountUDF(allStringCol) - 1)

      withLinesCount
        .withColumn(
          "previousNewLines",
          coalesce(
            sum($"newlines").over(
              Window
                .orderBy($"logicalNumber")
                .rangeBetween(start = Window.unboundedPreceding, end = Window.currentRow - 1)
            ),
            lit(0)
          )
        )
        .select(
          dfCols :+ ($"logicalNumber" + $"previousNewLines").as(SOURCE_LINE_NUMBER_COL_NAME): _*
        )
    } else {
      // with no multiline the logical numbering will match the true line number so avoid the window function
      logicallyNumberedDF.withColumnRenamed("logicalNumber", SOURCE_LINE_NUMBER_COL_NAME)
    }

    numberedDF
  }
}
