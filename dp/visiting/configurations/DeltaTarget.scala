package com.bhp.dp.visiting.configurations

import com.bhp.dp.utils.FileSystemUtils
import com.bhp.dp.utils.Snapshot.SNAPSHOT_PARTITION_KEY
import com.bhp.dp.visiting.components.{TargetConfig, WriteInfo}
import com.bhp.dp.visiting.utils.DataframeMetadata
import com.bhp.dp.visiting.utils.SnapshotDateReader.SOURCE_SNAPSHOT_KEY
import com.google.common.cache.{Cache, CacheBuilder}
import io.delta.exceptions.DeltaConcurrentModificationException
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import java.util.concurrent.Semaphore
import scala.concurrent.duration.DurationInt
import scala.util.matching.Regex

case class DeltaTarget(
    hiveDbName: String,
    tableName: String,
    deltaDir: String,
    saveMode: SaveMode,
    numPartitions: Option[Int] = None,
    partitionColumns: Option[Seq[String]] = None,
    upsertKeyColumns: Option[Seq[String]] = None,
    deleteOnNoMatch: Option[Boolean] = None
) extends TargetConfig
    with Logging {

  import DeltaTarget._
  import ParquetBase._

  hiveDbName match {
    case validRegex() => // good name
    case _ => throw new IllegalArgumentException(s"$hiveDbName does not match regex $validRegex")
  }

  tableName.replace("{type}", "type") match {
    case validRegex() => // good name
    case _ => throw new IllegalStateException(s"$tableName does not match regex $validRegex")
  }

  if (saveMode != SaveMode.Append && upsertKeyColumns.exists(_.nonEmpty)) {
    throw new IllegalArgumentException(
      s"Must use Append saveMode instead of $saveMode to set upsertKeyColumns ${upsertKeyColumns.get
        .mkString("[", ",", "]")}"
    )
  }

  if (partitionColumns.exists(_.isEmpty)) {
    throw new IllegalArgumentException("partitionColumns cannot be empty")
  }

  if (upsertKeyColumns.exists(_.isEmpty)) {
    throw new IllegalArgumentException("upsertKeyColumns cannot be empty")
  }

  // if upserting is enabled, ANY concurrent modifications will cause an exception. best to just limit threading.
  val maxThreadsAllowed = if (upsertKeyColumns.exists(_.nonEmpty)) 1 else MAX_THREADS_PER_DELTA

  override def targetType: String = "delta"

  override def configurationSummary: String =
    s"Database=$hiveDbName, Table=$tableName, DeltaDir=$deltaDir, " +
      s"PartitionColumns=${partitionColumns.map(_.mkString("[", ",", "]")).getOrElse("None")}, " +
      s"UpsertKeyColumns=${upsertKeyColumns.map(_.mkString("[", ",", "]")).getOrElse("None")}, " +
      s"MaxThreads=$maxThreadsAllowed"

  override protected def additionalTokens: Map[String, String] = Map(
    "hiveDbName" -> hiveDbName,
    "tableName"  -> tableName,
    "deltaDir"   -> deltaDir
  )

  // Delta has problems with concurrent writes.
  // Prevent more than maxThreadsAllowed from writing to a particular table at a time
  private def getSemaphore(path: String): Semaphore =
    semaphoreCache.get(path, () => new Semaphore(maxThreadsAllowed))

  override def deleteData(
      sparkSession: SparkSession,
      deleteTokens: Map[String, String],
      metaFileName: Option[String]
  ): Unit = {
    // drop possible view and _history table
    val table        = getFinalTableName(tableName, deleteTokens)
    val historyTable = s"${table}_history"
    val fs           = FileSystemUtils.getFileSystem(sparkSession)

    try {
      logger.info(s"Dropping view $hiveDbName.$table")
      sparkSession.sql(s"drop view if exists $hiveDbName.$table")
      logger.info(s"Dropping table $hiveDbName.$historyTable")
      sparkSession.sql(s"drop table if exists $hiveDbName.$historyTable")
    } catch {
      case e: AnalysisException =>
        if (e.message.contains("Please use DROP TABLE instead")) {
          // do nothing. table is likely a delta table and will be handled by later logic
        } else {
          throw e
        }
    }

    import sparkSession.implicits._

    logger.info(
      s"Performing delete for $configurationSummary with tokens $deleteTokens and metaFileName ${metaFileName
        .getOrElse("None")}"
    )

    val fullPath       = getFullPath(deleteTokens, deltaDir, "")
    val fullPathHadoop = new Path(fullPath)
    val semaphore      = getSemaphore(fullPath)

    if (fs.exists(fullPathHadoop)) {
      if (saveMode == SaveMode.Overwrite) {
        logger.info(s"Detected SaveMode.Overwrite reingest, dropping $hiveDbName.$table")
        sparkSession.sql(s"drop table if exists $hiveDbName.$table")
        logger.info(s"$fullPath is in SaveMode.Overwrite. Deleting entire folder: $fullPath")
        fs.delete(fullPathHadoop, true)
      } else {
        metaFileName match {
          case Some(fileName) =>
            // specific file reingest
            val snapshotIndex       = fileName.indexOf(SNAPSHOT_PARTITION_KEY)
            val snapshotAndFilename = fileName.substring(snapshotIndex)

            logger.info(
              s"Will delete rows where meta_source_name.endsWith($snapshotAndFilename) from $fileName"
            )

            semaphore.acquire()
            try {
              DeltaTable
                .forPath(sparkSession, fullPath)
                .delete($"meta_source_name".endsWith(lit(snapshotAndFilename)))

            } finally {
              semaphore.release()
            }

          case None =>
            if (deleteTokens.contains(SOURCE_SNAPSHOT_KEY)) {
              // snapshot_date folder reingest
              val snapshotDate = deleteTokens(SOURCE_SNAPSHOT_KEY)
              logger.info(s"Will delete from delta table where snapshot_date = $snapshotDate")

              semaphore.acquire()
              try {

                DeltaTable
                  .forPath(sparkSession, fullPath)
                  .delete($"$SNAPSHOT_PARTITION_KEY" === lit(snapshotDate))

              } finally {
                semaphore.release()
              }
            } else {
              // full history reingest
              logger.info(s"Detected full history reingest, dropping $hiveDbName.$table")
              sparkSession.sql(s"drop table if exists $hiveDbName.$table")
              logger.info(s"Deleting entire folder: $fullPath")
              fs.delete(fullPathHadoop, true)
            }
        }
      }
    } else {
      logger.info(s"$fullPath does not exist. Nothing to delete.")
    }
  }

  private def standardWrite(dataframe: DataFrame, fullPath: String, table: String): Unit = {
    var writer = dataframe.write
      .format("delta")
      .mode(saveMode)
      .option("overwriteSchema", value = true)
      .option("path", fullPath)

    writer = partitionColumns match {
      case Some(cols) => writer.partitionBy(cols: _*)
      case None       => writer
    }

    writer.save()
  }

  override def writeData(df: DataframeMetadata, ingestId: String): WriteInfo = {

    val sparkSession = df.dataFrame.sparkSession

    val table = getFinalTableName(tableName, df.tokens)

    val fullPath = getFullPath(df.tokens, deltaDir, "")

    val semaphore = getSemaphore(fullPath)

    semaphore.acquire()

    sparkSession.sql(s"create database if not exists $hiveDbName")

    try {
      // add the snapshot_date to the dataframe and coalesce
      val dataframe =
        df.dataFrame
          .withColumn(SNAPSHOT_PARTITION_KEY, lit(getSnapshotString(df.tokens)))
          .coalesce(numPartitions.getOrElse(8))

      var doRetry = false

      do {
        doRetry = false
        try {
          upsertKeyColumns match {
            case Some(keyColumns) =>
              // has upsertKeyColumns. do Delta write.

              val mergeCondition =
                keyColumns.map(c => s"existing.$c == updates.$c").mkString(" and ")
              try {
                var mergeBuilder = DeltaTable
                  .forPath(sparkSession, fullPath)
                  .as("existing")
                  .merge(dataframe.as("updates"), mergeCondition)
                  .whenMatched("existing.snapshot_date <= updates.snapshot_date")
                  .updateAll()
                  .whenNotMatched()
                  .insertAll()

                if (deleteOnNoMatch.getOrElse(false)) {
                  mergeBuilder = mergeBuilder.whenNotMatchedBySource().delete()
                }

                mergeBuilder.execute()
              } catch {
                case e: Exception =>
                  if (e.getMessage.toLowerCase.contains("not a delta table")) {
                    logger.warn(
                      "Caught 'not a delta table' exception during merge. Retrying as standard write."
                    )
                    // possibly not created yet. retry with standard write
                    standardWrite(dataframe, fullPath, table)
                  } else {
                    throw e
                  }
              }

            case None =>
              // no upsertKeyColumns. do standard write
              standardWrite(dataframe, fullPath, table)
          }
        } catch {
          case e: DeltaConcurrentModificationException =>
            // all retriable exceptions extend DeltaConcurrentModificationException (and as of Feb 22, 2023, only those)
            logger.warn(s"Caught Delta ${e.getClass.getSimpleName}. Sleeping 2sec. Retrying.", e)
            Thread.sleep(2.seconds.toMillis)
            doRetry = true
        }
      } while (doRetry)

      sparkSession.sql(
        s"""CREATE TABLE IF NOT EXISTS $hiveDbName.$table USING DELTA LOCATION "$fullPath""""
      )

    } finally {
      semaphore.release()
    }

    WriteInfo(
      fullPath,
      Some(
        Map(
          "hiveDatabase"  -> hiveDbName,
          "hiveTableName" -> table
        )
      )
    )
  }

}

object DeltaTarget {
  private val validRegex: Regex = "[a-zA-z_0-9/.]+".r

  // limit the number of threads updating a specific delta table at a time
  private val MAX_THREADS_PER_DELTA = 2

  private val semaphoreCache: Cache[String, Semaphore] = CacheBuilder
    .newBuilder()
    .maximumSize(500)
    .concurrencyLevel(1)
    .build()
}
