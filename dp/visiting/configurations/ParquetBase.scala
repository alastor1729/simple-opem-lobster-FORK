package com.bhp.dp.visiting.configurations

import com.bhp.dp.utils.{FileSystemUtils, Snapshot}
import com.bhp.dp.utils.Snapshot.SNAPSHOT_PARTITION_KEY
import com.bhp.dp.visiting.components.{TargetConfig, WriteInfo}
import com.bhp.dp.visiting.utils.{DataframeMetadata, SnapshotDateReader}
import com.bhp.dp.visiting.utils.SnapshotDateReader.SOURCE_SNAPSHOT_KEY
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

abstract class ParquetBase(parquetDir: String, saveMode: SaveMode, numPartitions: Option[Int])
    extends TargetConfig
    with Logging {

  import ParquetBase._

  def appendedExtension: String

  def postWriteTask(df: DataframeMetadata, fullPath: String): WriteInfo = WriteInfo(fullPath)

  override def deleteData(
      sparkSession: SparkSession,
      deleteTokens: Map[String, String],
      metaFileName: Option[String] = None
  ): Unit = {

    import sparkSession.implicits._

    logger.info(
      s"Performing delete for $configurationSummary with tokens $deleteTokens and metaFileName ${metaFileName
        .getOrElse("None")}"
    )

    val fs = FileSystemUtils.getFileSystem(sparkSession)

    val fullPath = getFullPath(deleteTokens, parquetDir, appendedExtension)

    val deletePath = if (deleteTokens.contains(SOURCE_SNAPSHOT_KEY)) {
      val timeString =
        SnapshotDateReader.readSnapshotFromFilePath(deleteTokens(SOURCE_SNAPSHOT_KEY))
      s"$fullPath/${SNAPSHOT_PARTITION_KEY}=$timeString"
    } else {
      fullPath
    }

    val toDelete = metaFileName match {
      case Some(fileName) =>
        if (saveMode == SaveMode.Overwrite) {
          logger.info(s"$deletePath is in SaveMode.Overwrite. Will delete entire folder.")
          Seq(deletePath)
        } else {
          val deletePathHdfs = new Path(deletePath)
          if (fs.exists(deletePathHdfs)) {
            val snapshotIndex       = fileName.indexOf(SNAPSHOT_PARTITION_KEY)
            val snapshotAndFilename = fileName.substring(snapshotIndex)

            logger.info(
              s"Will delete specific parquet files for meta_source_name.endsWith($snapshotAndFilename) from $fileName"
            )

            val parquetFiles = FileSystemUtils
              .listPaths(sparkSession, deletePath)
              .filter(_.toString.endsWith(".parquet"))

            parquetFiles
              .filterNot(p => {
                sparkSession.read
                  .parquet(p.toString)
                  .where($"meta_source_name".endsWith(lit(snapshotAndFilename)))
                  .isEmpty
              })
              .map(_.toString)
          } else {
            logger.info(s"$deletePath does not exist. Nothing to delete.")
            Seq.empty
          }
        }
      case None =>
        logger.info(s"Will delete entire folder: $deletePath")
        Seq(deletePath)
    }

    toDelete.foreach(d => {
      val deletePath = new Path(d)
      if (fs.exists(deletePath)) {
        logger.info(s"Deleting $d")
        fs.delete(deletePath)
      }
    })

  }

  override def writeData(df: DataframeMetadata, ingestId: String): WriteInfo = {

    val sparkSession = df.dataFrame.sparkSession

    val fullPath = getFullPath(df.tokens, parquetDir, appendedExtension)

    val sourceDate = getSourceDate(df.tokens)

    try {
      Snapshot.WriteSnapshotPartitionToPath(
        sparkSession,
        df.dataFrame,
        sourceDate,
        fullPath,
        numPartitions.getOrElse(8),
        saveMode = saveMode
      )
    } catch {
      case exception: Throwable =>
        // check depth to avoid infinite loop
        var depth         = 0
        var cause         = exception
        var foundParallel = false
        while (cause != null && depth < 10 && !foundParallel) {
          foundParallel = cause.getMessage.contains("_SUCCESS") && cause.getMessage.contains(
            "Parallel access to the create path detected"
          )
          cause = cause.getCause
          depth = depth + 1
        }

        if (foundParallel) {
          // this is an Azure issue around writing the 0 byte _SUCCESS file --> warn only
          logger.warn(s"$ingestId: Suppressing Azure parallel access exception for $fullPath")
        } else {
          // some other exception. throw
          throw exception
        }
    }

    postWriteTask(df, fullPath)
  }

}

object ParquetBase {
  import TargetConfig.replaceTokens

  def getFinalTableName(tableName: String, tokens: Map[String, String]) =
    replaceTokens(tableName, tokens)

  /** This helps protect against a change in Spark >=3.1.2. In Spark 3.0.1, "./folder/target"
    * replaced the "." with the current folder. In Spark >=3.1.2 the "." is replaced with the
    * spark_warehouse directory. Not a big deal in dev and prod since we don't ever use prepended
    * ".", but lots of unit tests do.
    *
    * @param path
    * @return
    */
  def getUnixPath(path: String): String = {
    FilenameUtils.separatorsToUnix(if (path.startsWith(".")) {
      new File(path).getCanonicalPath
    } else {
      path
    })
  }

  def getFullPath(
      tokens: Map[String, String],
      parquetDir: String,
      appendedExtension: String
  ): String = {
    val dir = replaceTokens(parquetDir, tokens)

    // have to keep the extension due to historical usage
    getUnixPath(s"${dir.stripSuffix("/")}$appendedExtension")
  }

  def getSnapshotString(tokens: Map[String, String]): String = tokens(SOURCE_SNAPSHOT_KEY)

  def getSourceDate(tokens: Map[String, String]): LocalDateTime = LocalDateTime.parse(
    getSnapshotString(tokens),
    DateTimeFormatter.ofPattern(Snapshot.SNAPSHOT_DATE_FORMAT_STRING)
  )
}
