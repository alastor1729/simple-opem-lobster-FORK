package com.bhp.dp.visiting.configurations

import com.bhp.dp.utils.{Hive, Snapshot}
import com.bhp.dp.visiting.components.WriteInfo
import com.bhp.dp.visiting.utils.DataframeMetadata
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}

import scala.util.matching.Regex

case class HiveTarget(
    hiveDbName: String,
    tableName: String,
    hiveDir: String,
    saveMode: SaveMode,
    numPartitions: Option[Int] = None
) extends ParquetBase(hiveDir, saveMode, numPartitions)
    with Logging {

  import HiveTarget._
  import ParquetBase._

  hiveDbName match {
    case validRegex() => // good name
    case _ => throw new IllegalArgumentException(s"$hiveDbName does not match regex $validRegex")
  }

  tableName.replace("{type}", "type") match {
    case validRegex() => // good name
    case _ => throw new IllegalStateException(s"$tableName does not match regex $validRegex")
  }

  override def targetType: String = "hive"

  override def additionalTokens: Map[String, String] = {
    Map(
      "hiveDir"    -> hiveDir,
      "hiveDbName" -> hiveDbName,
      "tableName"  -> tableName
    )
  }

  override def configurationSummary: String =
    s"Database=$hiveDbName, Table=$tableName, ParquetDir=$hiveDir"

  override def appendedExtension: String = "_history.parquet"

  override def deleteData(
      sparkSession: SparkSession,
      deleteTokens: Map[String, String],
      metaFileName: Option[String]
  ): Unit = {
    val table        = getFinalTableName(tableName, deleteTokens)
    val historyTable = s"${table}_history"

    try {
      logger.info(s"Dropping view $hiveDbName.$table")
      sparkSession.sql(s"drop view if exists $hiveDbName.$table")
    } catch {
      case e: AnalysisException =>
        if (e.message.contains("Cannot drop a table with DROP VIEW")) {
          sparkSession.sql(s"drop table if exists $hiveDbName.$table")
        } else {
          throw e
        }
    }

    logger.info(s"Dropping table $hiveDbName.$historyTable")
    sparkSession.sql(s"drop table if exists $hiveDbName.$historyTable")

    super.deleteData(sparkSession, deleteTokens, metaFileName)
  }

  override def postWriteTask(df: DataframeMetadata, fullPath: String): WriteInfo = {

    val baseWriteInfo = super.postWriteTask(df, fullPath)

    val sparkSession = df.dataFrame.sparkSession

    val table = getFinalTableName(tableName, df.tokens)

    val historyTableName = s"${table}_history"

    Hive.createOrUpdateSpecifiedTableFromParquet(
      sparkSession = sparkSession,
      dbName = hiveDbName,
      tableName = historyTableName,
      parquetFullPath = fullPath,
      partitionKey = Snapshot.SNAPSHOT_PARTITION_KEY,
      specifiedSchema = Some(df.dataFrame.schema),
      partitionInfo = Some(Seq(Snapshot.SNAPSHOT_PARTITION_KEY -> getSnapshotString(df.tokens)))
    )

    Hive.createOrReplaceViewFromPartitionedTable(
      sparkSession,
      hiveDbName,
      historyTableName,
      table
    )

    val finalDetails = baseWriteInfo.details.getOrElse(Map.empty) ++
      Map(
        "hiveDatabase"  -> hiveDbName,
        "hiveTableName" -> historyTableName,
        "hiveViewName"  -> table
      )

    baseWriteInfo.copy(details = Some(finalDetails))
  }
}

object HiveTarget {
  val validRegex: Regex = "[a-zA-z_0-9/.]+".r
}
