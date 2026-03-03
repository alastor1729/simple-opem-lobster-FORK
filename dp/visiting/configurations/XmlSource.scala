package com.bhp.dp.visiting.configurations

import com.bhp.dp.visiting.components.SourceConfig
import com.bhp.dp.visiting.utils.{DataframeMetadata, SchemaMutations, SourceData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

case class XmlSource(sourceSystem: String, types: Seq[XmlRowType]) extends SourceConfig {

  if (types.isEmpty)
    throw new IllegalArgumentException("XmlSource.types is required to not be empty")

  import SourceConfig._

  override def sourceType: String = "xml"

  override def configurationSummary: String =
    s"System=$sourceSystem, Types=[${types.map(x => s"(Tag=${x.rowTag}, Flatten=${x.flatten})").mkString(",")}]"

  private def generateTokens(filePath: String, xmlRowType: XmlRowType): Map[String, String] = {
    Map(
      "sourceLocation" -> filePath,
      "sourceOffset"   -> xmlRowType.rowTag,
      "type"           -> xmlRowType.rowTag,
      "isFlat"         -> xmlRowType.flatten.getOrElse(false).toString
    )
  }

  override def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]] =
    types.map(t => generateTokens(filePath, t))

  override def getSourceData(
      sparkSession: SparkSession,
      filePath: String,
      ingestId: String
  ): SourceData = {
    SourceData(
      types.map(rowType => {

        val commonDf = sparkSession.read
          .format("com.databricks.spark.xml")
          .option(
            "excludeAttribute",
            value = false
          ) // whether or not to exclude attributes in the tag. Default false.
          .option("nullValue", "") // the value to treat as a null. Default "".
          .option(
            "attributePrefix",
            "_"
          ) // prefix in column name for all attributes. Default _. Cannot be empty.
          .option(
            "valueTag",
            "_VALUE"
          ) // Tag used for the element value (when no child elements and has attributes). Default _VALUE
          .option(
            "ignoreSurroundingSpaces",
            value = false
          )                           // Whitespace surrounding the value. Default false.
          .option("charset", "UTF-8") // Default UTF-8

        val reader = rowType.schema match {
          case Some(schema) => commonDf.schema(schema)
          case None         => commonDf.option("inferSchema", value = true)
        }

        val data          = reader.option("rowTag", rowType.rowTag).load(filePath)
        val flattenedData = if (rowType.flatten.getOrElse(false)) flattenData(data) else data
        // Make primitives as string by default, otherwise use config.
        // Doing rename here because castTypesToString does rename (no special characters in cast strings) and I want output of this method to be consistent.
        val postProcessedData = if (rowType.primitivesAsString.getOrElse(true)) {
          SchemaMutations.castTypesToString(flattenedData)
        } else {
          SchemaMutations.rename(flattenedData)
        }

        DataframeMetadata(postProcessedData, generateTokens(filePath, rowType))
      })
    )
  }
}

case class XmlRowType(
    rowTag: String,
    flatten: Option[Boolean] = None,
    primitivesAsString: Option[Boolean] = None,
    schema: Option[StructType] = None
)
