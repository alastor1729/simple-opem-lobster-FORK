package com.bhp.dp.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object FileSystemUtils {

  def getFileSystem(sparkSession: SparkSession): FileSystem = {
    if (RawLoaderJobUtils.isLocal) {
      new Path("./").getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    } else {
      new Path("abfss://datalake-engineering@neudatalakeprsa01.dfs.core.windows.net/")
        .getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    }
  }

  def listPaths(sparkSession: SparkSession, dir: String, recursive: Boolean = false): Seq[Path] = {
    val iter = getFileSystem(sparkSession).listFiles(new Path(dir), recursive)

    val lb = ListBuffer.empty[Path]
    while (iter.hasNext) {
      val n = iter.next()
      lb += n.getPath
    }

    lb.toList
  }
}
