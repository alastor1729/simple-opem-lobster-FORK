package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.{DataframeMetadata, FileDecompressor, SchemaMutations, SourceData}
import com.crealytics.spark.excel.WorkbookReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

import scala.util.matching.Regex

case class ExcelFullFileSource(
    sourceSystem: String,
    sheetNameColumn: String,
    sheetOptions: Seq[SheetOptions],
    implementation: Option[String] = None
) extends SourceConfig {

  if (sheetOptions.isEmpty)
    throw new IllegalArgumentException(
      "ExcelFullFileSource.sheetOptions is required to not be empty"
    )

  sheetOptions.foreach(s => s.columns.foreach(validateColumns))

  override def sourceType: String = "excel_full_file"

  override def configurationSummary: String =
    s"System=$sourceSystem, SheetNameColumn=$sheetNameColumn, DataAddress=[${sheetOptions.map(o => o.GetDataAddress(o.sheetNameRegex)).mkString(", ")}]"

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

    import sparkSession.implicits._

    val excelFilePath = if (filePath.contains(".xlsx.bz2")) {
      FileDecompressor.fileDecompressor(
        sparkSession,
        filePath
      ) // decompress a bz2 file and return newfilepath.
    } else {
      filePath
    }

    val sheetNames = WorkbookReader(
      Map(
        "path" -> filePath
        // this prevents reading the entire file into memory just for sheet names but is not supported for older xls files
      ) ++ (if (filePath.endsWith(".xls")) Map.empty else Map("maxRowsInMemory" -> "10")),
      sparkSession.sparkContext.hadoopConfiguration
    ).sheetNames

    val dataFrames = sheetNames.flatMap(sheetName => {

      val optsToUse = sheetOptions.foldLeft(Option.empty[(SheetOptions, Regex.Match)]) {
        case (f, current) =>
          f match {
            case Some(_) => f
            case None    => current.regexPattern.findFirstMatchIn(sheetName).map((current, _))
          }
      }

      optsToUse.map { case (o, matchInfo) =>
        val sheetNameForColumn = o.customSheetName match {
          case Some(n) => n // if custom is provided use that
          case None =>
            if (matchInfo.subgroups.nonEmpty) {
              matchInfo.subgroups.head // if a capturing group is specified use the first one
            } else {
              sheetName // use the name from the Excel file
            }
        }

        val df = ExcelSource
          .getExcelDF(
            sparkSession,
            excelFilePath,
            o.hasHeader,
            o.GetDataAddress(sheetName),
            required = true,
            implementation
          )
          .withColumn(sheetNameColumn, lit(sheetNameForColumn))

        val renamedDf = o.columns match {
          case Some(cols) =>
            if (cols.contains(sheetNameColumn)) {
              SchemaMutations.rename(df, cols)
            } else {
              SchemaMutations.rename(df, cols :+ sheetNameColumn)
            }

          case None => df
        }

        // need to clean names here for the unionByName
        SchemaMutations.rename(renamedDf)
      }
    })

    val combinedDF = if (dataFrames.isEmpty) {
      // cannot reduce an empty Seq so spit out an empty DF
      Seq.empty[String].toDF("emptyOutput")
    } else {
      dataFrames.reduce[DataFrame] { case (l, r) =>
        // don't bother unioning empty DataFrames -- could cause column naming conflicts
        if (l.isEmpty) {
          r
        } else if (r.isEmpty) {
          l
        } else {
          l.unionByName(r)
        }
      }
    }

    SourceData(DataframeMetadata(combinedDF, generateTokens(filePath)))
  }
}

case class SheetOptions(
    customSheetName: Option[String] = None,
    hasHeader: Boolean,
    sheetNameRegex: String,
    startCell: String,
    endCell: Option[String] = None,
    columns: Option[Seq[String]] = None
) {

  // this forces validation of the regex on load (and unit test!). will prevent runtime errors if invalid regex provided
  val regexPattern: Regex = sheetNameRegex.r

  def GetDataAddress(sheetName: String): String = {
    s"'$sheetName'!$startCell${endCell.fold("")(x => s":$x")}" // '<sheet>'!<start>:<end>
  }
}
