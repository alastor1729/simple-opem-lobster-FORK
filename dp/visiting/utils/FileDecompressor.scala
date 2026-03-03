package com.bhp.dp.visiting.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession

import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory

object FileDecompressor {

  /** Decompress a .xlsx.bz2 file format with this utility. It will return the new decompressed
    * local file path. Local file will be removed once the data file is written at target location.
    *
    * @param spark
    *   session.
    * @param sourcePath
    *   Compressed bz2 excel file path.
    * @return
    *   Decompressed excel local file path.
    */
  def fileDecompressor(spark: SparkSession, sourcePath: String): String = {
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val conf           = spark.sparkContext.hadoopConfiguration
    val inputPath      = new Path(sourcePath)
    val factory        = new CompressionCodecFactory(conf)
    val codec          = factory.getCodec(inputPath)

    // create local dir to store the decompressed file
    val xlsxFile    = inputPath.getName.replace(".bz2", "")
    val workingArea = s"/dbfs/tmp/${inputPath.getName}/"
    val finalName   = s"$workingArea$xlsxFile"
    deleteDirectory(workingArea) // delete directory if already exists
    new java.io.File(workingArea).mkdirs()
    val path1 = Paths.get(finalName)
    Files.createFile(path1)

    var in: InputStream   = null
    var out: OutputStream = null

    try {
      in = codec.createInputStream(fs.open(inputPath))
      out = new FileOutputStream(finalName)
      IOUtils.copyBytes(in, out, conf)
    } finally {
      IOUtils.closeStream(in)
      IOUtils.closeStream(out)
    }
    s"file:$finalName"
  }

  /** Delete a temporary decompressed excel file at local path
    *
    * @param filePath
    */
  def deleteDirectory(directoryPath: String): Unit = {
    val directory = new Directory(new File(directoryPath))
    directory.deleteRecursively()
  }

}
