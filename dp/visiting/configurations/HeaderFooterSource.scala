package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.{DataframeMetadata, SourceData}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.{col, substring, trim}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

case class DelimiterType(
    typeName: String,
    delimiterWidth: Option[Int] = None,
    delimiterValue: String,
    columns: Seq[FixedWidthColumn]
)

case class ColumnsAndStartIndex(columns: Seq[Column] = Seq.empty, startIndex: Int = 1)

case class TypeInfoAndColumns(
    typeName: String,
    delimiterWidth: Int,
    typeDelimiter: String,
    columns: Seq[Column]
)

case class HeaderFooterSource(
    sourceSystem: String,
    delimiterIndex: Int,
    delimiterWidth: Int,
    types: Seq[DelimiterType]
) extends SourceConfig {

  if (types.isEmpty)
    throw new IllegalArgumentException("HeaderFooterSource.types is required to not be empty")

  if (types.exists(_.columns.isEmpty))
    throw new IllegalArgumentException(
      "HeaderFooterSource.types.columns is required to not be empty"
    )

  types.foreach(t => validateColumns(t.columns.map(_.name)))

  import SourceConfig._

  private val typeToColumnMap = types.map(t => {
    val columns = t.columns.foldLeft(new ColumnsAndStartIndex)((cAcc, cCur) => {
      val column = trim(substring(col("value"), cAcc.startIndex, cCur.width)).alias(cCur.name)
      cAcc.copy(columns = cAcc.columns :+ column, cCur.width + cAcc.startIndex)
    })

    TypeInfoAndColumns(
      t.typeName,
      t.delimiterWidth.getOrElse(delimiterWidth),
      t.delimiterValue,
      columns.columns
    )
  })

  override def sourceType: String = "headerFooter"

  override protected def additionalTokens: Map[String, String] = {
    Map(
      "delimiterIndex" -> delimiterIndex.toString,
      "delimiterWidth" -> delimiterWidth.toString
    )
  }

  override def configurationSummary: String =
    s"System=$sourceSystem, DelimiterIndex=$delimiterIndex, DelimiterWidth=$delimiterWidth, Types=[${types
      .map(t => s"(${t.typeName}, ${t.delimiterValue}, ${t.columns.size} cols)")
      .mkString(", ")}]"

  private def generateTokens(
      typeInfoAndColumns: TypeInfoAndColumns,
      filePath: String
  ): Map[String, String] = {
    Map(
      "sourceLocation" -> filePath,
      "sourceOffset"   -> typeInfoAndColumns.columns.mkString(", "),
      "type"           -> typeInfoAndColumns.typeName
    )
  }

  override def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]] = {
    typeToColumnMap.map(x => generateTokens(x, filePath))
  }

  override def getSourceData(
      sparkSession: SparkSession,
      filePath: String,
      ingestId: String
  ): SourceData = {

    val rdd = sparkSession.sparkContext.textFile(filePath).zipWithIndex().map { case (line, idx) =>
      Row.fromSeq(Seq(line, idx + 1))
    }

    val df = sparkSession.createDataFrame(
      rdd,
      StructType(
        Seq(StructField("value", StringType), StructField(SOURCE_LINE_NUMBER_COL_NAME, LongType))
      )
    )

    SourceData(
      typeToColumnMap.map(x => {
        DataframeMetadata(
          df.withColumn(
            "delimiter_value",
            substring(col("value"), delimiterIndex, x.delimiterWidth)
          ).filter(trim(col("delimiter_value")).rlike(x.typeDelimiter))
            .select(x.columns :+ col(SOURCE_LINE_NUMBER_COL_NAME): _*),
          generateTokens(x, filePath)
        )
      })
    )
  }

}
