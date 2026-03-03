package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.{DataframeMetadata, SourceData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

case class FixedWidthSource(sourceSystem: String, columns: Seq[FixedWidthColumn])
    extends SourceConfig {

  if (columns.isEmpty)
    throw new IllegalArgumentException("FixedWidthSource.columns is required to not be empty")

  validateColumns(columns.map(_.name))

  import SourceConfig._

  override def sourceType: String = "fixedWidth"

  override def configurationSummary: String =
    s"System=$sourceSystem, Columns=[${columns.map(x => s"(Name=${x.name}, Width=${x.width})").mkString(", ")}]"

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
    val rdd: RDD[String] = sparkSession.sparkContext.textFile(filePath)

    val fields = columns.map(fwc =>
      StructField(fwc.name, StringType, nullable = true)
    ) :+ StructField(SOURCE_LINE_NUMBER_COL_NAME, LongType, nullable = true)

    val schema = StructType(fields)

    val splitStringIntoRow: (Seq[FixedWidthColumn], String, Long) => Row =
      (list: Seq[FixedWidthColumn], str: String, idx: Long) => {
        val (_, result) =
          list.map(x => x.width).foldLeft((str, List[String]())) { case ((s, res), curr) =>
            if (s.length() <= curr) {
              val split = s.substring(0).trim()
              val rest  = ""
              (rest, split :: res)
            } else if (s.length() > curr) {
              val split = s.substring(0, curr).trim()
              val rest  = s.substring(curr)
              (rest, split :: res)
            } else {
              val split = ""
              val rest  = ""
              (rest, split :: res)
            }
          }
        Row.fromSeq(result.reverse :+ (idx + 1))
      }

    SourceData(
      DataframeMetadata(
        sparkSession.createDataFrame(
          rdd.zipWithIndex().map { case (line, idx) =>
            splitStringIntoRow(columns, line, idx)
          },
          schema
        ),
        generateTokens(filePath)
      )
    )
  }
}
