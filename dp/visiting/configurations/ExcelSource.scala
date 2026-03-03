package com.bhp.dp.visiting.configurations


import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.{DataframeMetadata, FileDecompressor, SchemaMutations, SourceData}
import com.github.pjfanning.xlsx.exceptions.MissingSheetException
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.scala.Logging
import org.apache.poi.openxml4j.exceptions.NotOfficeXmlFileException
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import java.io.IOException


case class ExcelSource(
    sourceSystem: String,
    excelRange: Seq[ExcelRange],
    implementation: Option[String] = None
) extends SourceConfig {

  if (excelRange.isEmpty)
    throw new IllegalArgumentException("ExcelSource.excelRange is required to not be empty")

  excelRange.foreach(r => r.headerColumns.foreach(validateColumns))

  import ExcelSource._

  override def sourceType: String = "excel"

  override def configurationSummary: String =
    s"System=$sourceSystem, DataAddress=[${excelRange.map(_.GetDataAddress()).mkString(", ")}]"

  private def generateTokens(filePath: String, excelRange: ExcelRange): Map[String, String] = {
    Map(
      "sourceLocation" -> filePath,
      "type"           -> excelRange.typeName,
      "sourceOffset"   -> excelRange.GetDataAddress(),
      "sheet"          -> excelRange.sheet.getOrElse("Sheet1"),
      "startCell"      -> excelRange.startCell,
      "endCell"        -> excelRange.endCell.getOrElse("unbound"),
      "required"       -> excelRange.required.getOrElse(true).toString
    )
  }

  override def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]] =
    excelRange.map(range => generateTokens(filePath, range))

  override def getSourceData(
      sparkSession: SparkSession,
      filePath: String,
      ingestId: String): SourceData = {

    val excelFilePath =
      if (filePath.contains(".xlsx.bz2")) {
        FileDecompressor.fileDecompressor(
          sparkSession,
          filePath) // decompress a bz2 file and return newfilepath.
      } else {
        filePath
      }

    SourceData(
      excelRange.map(range => {
        val df = getExcelDF(
          sparkSession,
          excelFilePath,
          range.hasHeader,
          range.GetDataAddress(),
          range.required.getOrElse(true),
          implementation
        )

        val renamedDf = range.headerColumns match {
          case Some(cols) => SchemaMutations.rename(df, cols)
          case None       => df
        }

        DataframeMetadata(renamedDf, generateTokens(filePath, range))
      })
    )
  }
  //TODO -- end of def getSourceData Function~~~~
}


/****
  Data Addresses
    * The location of data to read or write can be specified
    * with the dataAddress option.

  * Currently the following address styles are supported:
  *
  *
  * B3: Start cell of the data. Reading will return all rows below and all columns to the right.
  * Writing will start here and use as many columns and rows as required.
  *
  *
  * B3:F35: Cell range of data. Reading will return only rows and columns in the specified range.
  * Writing will start in the first cell (B3 in this xede-scala) and use only the specified columns
  * and rows. If there are more rows or columns in the DataFrame to write, they will be truncated.
  * Make sure this is what you want.
  *
  *
  *'My Sheet'!B3:F35: Same as above, but with a specific sheet.
  *
  *
  * MyTable[#All]: Table of data. Reading will return all rows and columns in this table. Writing
  * will only write within the current range of the table. No growing of the table will be
  * performed. PRs to change this are welcome.
  *
  *
  * @param sheet
  * @param startCell
  * @param endCell
  */
  case class ExcelRange(
      typeName: String,
      hasHeader: Boolean,
      sheet: Option[String] = None,
      startCell: String,
      endCell: Option[String] = None,
      headerColumns: Option[Seq[String]] = None,
      required: Option[Boolean] = None
  ) {
    def GetDataAddress(): String = { //TODO -- refactor / test this???????
      sheet.fold("")(x => s"'$x'!") + startCell + endCell.fold("")(x =>
        s":$x"
      ) // <sheet>!<start>:<end>
    }
  }


object ExcelSource extends Logging {

  def getExcelDF(
      sparkSession: SparkSession,
      filePath: String,
      hasHeader: Boolean,
      dataAddress: String,
      required: Boolean,
      implementation: Option[String]
  ): DataFrame = {

    val baseReader = sparkSession.read
      .format(implementation.getOrElse("com.crealytics.spark.excel"))
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option(
        "setErrorCellsToFallbackValues",
        "true"
      ) // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
      .option(
        "usePlainNumberFormat",
        "false"
      ) // Optional, default: false, If true, format the cells without rounding and scientific notations
      .option("inferSchema", "false") // Optional, default: false
      .option(
        "timestampFormat",
        "MM-dd-yyyy HH:mm:ss"
      ) // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from//    .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs//    .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .option("header", hasHeader)
      .option("dataAddress", dataAddress)


    val xlsOrXlsx = if (filePath.endsWith(".xls")) {
      baseReader
    } else {
      baseReader.option(
        "maxRowsInMemory",
        20
      ) // Optional, default None. If set, uses a streaming reader which can help with big files. CANNOT BE USED WITH XLS
    }

    val outputDf = loadExcelDataFrame(required, filePath, xlsOrXlsx, sparkSession)

    if (!hasHeader || outputDf == sparkSession.emptyDataFrame) {
      outputDf
    } else {
      outputDf.drop(outputDf.columns.filter(x => x.startsWith("_c")): _*)
    }
  }

  def loadExcelDataFrame(
      required: Boolean,
      filePath: String,
      xlsOrXlsx: DataFrameReader,
      sparkSession: SparkSession
  ): DataFrame = {
    try {
      if (required) {
        xlsOrXlsx.load(filePath)
      } else {
        try {
          xlsOrXlsx.load(filePath)
        } catch {
          case _: MissingSheetException    => sparkSession.emptyDataFrame
          case _: IllegalArgumentException => sparkSession.emptyDataFrame
        }
      }
    } catch {
      case e: NotOfficeXmlFileException => handleBadFileException(e, filePath, sparkSession)
      case e: IOException               => handleBadFileException(e, filePath, sparkSession)
    }
  }

  def handleBadFileException(e: Exception, filePath: String, sparkSession: SparkSession) = {
    val p    = new Path(filePath)
    val name = p.getName

    if (name.startsWith("~$")) {
      logger.warn("Ignoring NotOfficeXmlFileException for file starting with ~$", e)
      sparkSession.emptyDataFrame
    } else {
      throw e
    }
  }

}
