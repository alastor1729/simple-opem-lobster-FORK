package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.SourceData
import org.apache.spark.sql.SparkSession

case class SqlServerSource(
    sourceSystem: String,
    tableOrQuery: String,
    server: String,
    database: String,
    port: Int,
    numPartitions: Int,
    fetchSize: Int
) extends SourceConfig {
  override def sourceType: String = "jdbc"

  override def configurationSummary: String = s"Server=$server, Database=$database, Port=$port"

  override def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]] =
    throw new NotImplementedError("SQL not yet implemented.")

  override def getSourceData(
      sparkSession: SparkSession,
      filePath: String,
      ingestId: String
  ): SourceData = {
    throw new NotImplementedError("SQL not yet implemented.")

//    val isSelectRegEx = raw"(?i)(?=.*\bSELECT\b)(?=.*\bFROM\b).*".r
//    val OLTP_JDBC_URL = ""
//
//    val jdbcUrl = makeJDBCUrl(OLTP_JDBC_URL, database)
//
//    val optionedReader = sparkSession.read
//                              .format("jdbc")
//                              .option("url", jdbcUrl)
//                              .option("numPartitions", numPartitions)
//                              .option("fetchsize", fetchSize)
//
//    val (optionKey, filename) = isSelectRegEx.findFirstIn(tableOrQuery).fold(("dbtable", tableOrQuery))(_ => ("query", "raw_sql_query"))
//    SourceData(optionedReader.option(optionKey, tableOrQuery).load(), filename)
  }

  final private def makeJDBCUrl(baseUrl: String, dbName: String): String = {
    val password = "" // dbutils.secrets.get(scope = "jdbc", key = "sql_password")
    val jdbcUrl  = baseUrl + "password=" + password + ";database=" + dbName + ";"
    jdbcUrl
  }

}
