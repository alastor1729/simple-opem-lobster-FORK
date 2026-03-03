package com.bhp.dp.visiting.configurations

import org.apache.spark.sql.SaveMode

case class ParquetTarget(
    parquetDir: String,
    saveMode: SaveMode,
    numPartitions: Option[Int] = None
) extends ParquetBase(parquetDir, saveMode, numPartitions) {

  override def targetType: String = "parquet"

  override def additionalTokens: Map[String, String] = Map(
    "saveMode"   -> saveMode.toString,
    "parquetDir" -> parquetDir
  )

  override def configurationSummary: String = s"ParquetDir=$parquetDir, SaveMode=$saveMode"

  override def appendedExtension: String = ".parquet"
}
