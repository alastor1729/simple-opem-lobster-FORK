package com.bhp.dp.visiting.utils

object SnapshotDateReader {
  val SOURCE_SNAPSHOT_KEY = "source_snapshot"

  def readSnapshotFromFilePath(filePath: String): String = {
    optionalSnapshotFromFilePath(filePath).getOrElse(
      throw new IllegalArgumentException(
        "source must have a snapshot date to follow datalake convention"
      )
    )
  }

  def optionalSnapshotFromFilePath(filePath: String): Option[String] = {
    val pattern = raw".*(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}).*".r

    filePath match {
      case pattern(snapshotMatch) => Some(snapshotMatch)
      case _                      => None
    }
  }
}
