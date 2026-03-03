package com.bhp.dp.visiting.utils

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame

object AppendLineage {
  def appendLineageToDf(tokens: Map[String, String], df: DataFrame) = {
    df
      .select(
        col("*"),
        lit(getHash(FilenameUtils.getBaseName(tokens("sourceLocation")))).as("meta_lineage_id"),
        lit(tokens("sourceSystem")).cast(StringType).as("meta_source_system_name"),
        lit(tokens("sourceLocation")).cast(StringType).as("meta_source_name"),
        lit(tokens("sourceOffset")).cast(StringType).as("meta_source_offset"),
        lit(tokens("datetime")).cast(StringType).as("meta_lineage_datetime"),
        lit(tokens("artifactId")).cast(StringType).as("meta_provenance_id"),
        lit("data-platform-datalake").as("meta_github_hash"),
        lit(tokens("version")).cast(StringType).as("meta_semver"),
        lit(tokens("datetime")).cast(StringType).as("meta_provenance_datetime")
      )
  }

  private def getHash(value: String): String = {
    val encoder = java.security.MessageDigest.getInstance("SHA1")
    BigInt(1, encoder.digest(value.getBytes())).toString(36).toUpperCase
  }
}
