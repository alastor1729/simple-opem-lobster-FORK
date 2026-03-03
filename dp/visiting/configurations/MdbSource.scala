package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.{DataframeMetadata, FileDecompressor, SourceData}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.SparkSession

case class MdbSource(
    sourceSystem: String,
    tables: Seq[String]
) extends SourceConfig
    with Logging {

  override def sourceType: String = "mdb"

  override def configurationSummary: String =
    s"System=$sourceSystem, tables=$tables"

  private def generateTokens(filePath: String, table: String): Map[String, String] = {
    Map(
      "sourceLocation" -> filePath,
      "type"           -> table.replace(" ", "_"),
      "sourceOffset"   -> ""
    )
  }

  override def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]] =
    tables.map(range => generateTokens(filePath, range))

  override def getSourceData(
      sparkSession: SparkSession,
      filePath: String,
      ingestId: String
  ): SourceData = {
    val newFilePath = (if (filePath.contains(".bz2")) {
                         FileDecompressor.fileDecompressor(
                           sparkSession,
                           filePath
                         ) // decompress a bz2 file and return newfilepath.
                       } else {
                         filePath
                       }).replace("file:", "")

    JdbcDialects.registerDialect(UcanaccessDialect)

    SourceData(
      tables.map(table => {

        logger.info("ingesting table: " + table)

        val df =
          sparkSession.read
            .format("jdbc")
            .option("url", s"jdbc:ucanaccess://$newFilePath")
            .option("dbtable", s"[$table]")
            .load()

        DataframeMetadata(df, generateTokens(newFilePath, table))

      })
    )
  }
}

object MdbSource extends Logging {}

import org.apache.spark.sql.jdbc.JdbcDialect

object UcanaccessDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(java.util.Locale.ROOT).startsWith("jdbc:ucanaccess")
  override def quoteIdentifier(colName: String): String = s"`$colName`"
}
