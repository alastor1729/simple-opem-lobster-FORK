package com.bhp.dp.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.SparkSession

import java.util.TimeZone

trait SparkJob {
  def buildSparkSession(appName: String): SparkSession = {
    val sessionBuilder = SparkSession.builder.appName(appName)

    if (RawLoaderJobUtils.isLocal) {
      sessionBuilder
        .master("local[*]")
        // these extensions need to be set for a local run but Databricks sets them automatically
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    }
    val session: SparkSession = sessionBuilder.getOrCreate
    // Azure can't handle multiple threads writing 0 bytes to _SUCCESS at the same time
    // have to set this here instead of in the original builder as "getOrCreate" could use an existing Spark session without this set
    session.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    // set this configuration for schema auto-migration during Delta merge
    session.conf.set("spark.databricks.delta.schema.autoMerge.enabled", true)
    setTimezone(session)

    if (!RawLoaderJobUtils.isLocal) {
      session.sparkContext.hadoopConfiguration.set(
        "fs.azure.account.key.neudatalakeprsa01.dfs.core.windows.net",
        dbutils.secrets.get("raw-loader", "DataLakeStorageAccountKey")
      )
    }

    session
  }

  def setTimezone(session: SparkSession): Unit = {
    System.setProperty("user.timezone", "UTC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    session.conf.set("spark.sql.session.timeZone", "UTC")
  }
}
