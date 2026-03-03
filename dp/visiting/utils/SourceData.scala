package com.bhp.dp.visiting.utils

import org.apache.spark.sql.DataFrame

case class SourceData(dataframe: Seq[DataframeMetadata])

case class DataframeMetadata(dataFrame: DataFrame, tokens: Map[String, String])

object SourceData {
  def apply(dataframeMetadata: DataframeMetadata): SourceData = new SourceData(
    Seq(dataframeMetadata)
  )
}
