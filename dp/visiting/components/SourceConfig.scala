package com.bhp.dp.visiting.components

import com.bhp.dp.visiting.utils.{SchemaMutations, SourceData}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.types.{ArrayType, StructType}

abstract class SourceConfig extends Config {

  def sourceType: String

  def sourceSystem: String

  protected def additionalTokens: Map[String, String] = Map.empty

  override def tokens: Map[String, String] = {
    Map(
      "sourceType"   -> sourceType,
      "sourceSystem" -> sourceSystem
    ) ++ additionalTokens
  }

  def getAllRuntimeTokens(filePath: String): Seq[Map[String, String]]

  def getSourceData(sparkSession: SparkSession, filePath: String, ingestId: String): SourceData

  protected def validateColumns(columnNames: Seq[String]): Unit = {
    val colInfo = columnNames.map(SchemaMutations.cleanColumnName)

    val duplicateColumns =
      colInfo.groupBy(s => s).map(x => x._1 -> x._2.length).filter(x => x._2 > 1)

    if (duplicateColumns.nonEmpty)
      throw new IllegalStateException(
        s"Duplicate (cleaned) columns ${duplicateColumns.keys.mkString("[", ", ", "]")} found in config $configurationSummary"
      )
  }
}

object SourceConfig {
  val SOURCE_LINE_NUMBER_COL_NAME = "meta_source_line_number"

  case class FlattenTarget(columnName: String, includeOtherColumns: Boolean)

  /** Flatten dataframes with nested/complex fields (struct types or arrays of any dimension). These
    * complex dataframes may have been formed by XML or JSON ingestion, or any complex type
    * ingestion.
    *
    * @param df
    *   The dataframe with complex fields.
    * @param flattenTargets
    *   If set, will limit the flatten to only these columns
    * @return
    *   A new dataframe where complex fields have been flattened into multiple columns.
    */
  final def flattenData(
      df: DataFrame,
      flattenTargets: Seq[FlattenTarget] = Seq.empty
  ): DataFrame = {
    var dfMutable: DataFrame = df
    var schema: StructType   = dfMutable.schema
    var schemaUpdated        = true
    while (schemaUpdated) {
      schemaUpdated = false
      schema.fields.foreach { elem =>
        val matchingFlatten = flattenTargets.find(_.columnName.toLowerCase == elem.name.toLowerCase)
        if (flattenTargets.isEmpty || matchingFlatten.nonEmpty) {
          val includeOtherCols = matchingFlatten.forall(_.includeOtherColumns)
          elem.dataType match {
            case _: ArrayType =>
              schemaUpdated = true

              val tempDF = dfMutable
                .withColumn(elem.name + "_temp", explode_outer(col(elem.name)))
                .drop(col(elem.name))

              dfMutable = if (includeOtherCols) {
                tempDF.withColumnRenamed(elem.name + "_temp", elem.name)
              } else {
                tempDF.select(col(elem.name + "_temp").as(elem.name))
              }

            case structType: StructType =>
              schemaUpdated = true
              // may have changed on a previous field in the current `foreach`
              // and we have to grab it again since this now does a select
              val currentSchema = dfMutable.schema
              val selectCols = currentSchema.fields.flatMap(f => {
                if (f.name == elem.name) {
                  // split out the StructType col
                  structType.fieldNames.map(n => col(s"${elem.name}.$n").as(s"${elem.name}_$n"))
                } else if (includeOtherCols) {
                  // if include = true then select this column
                  Seq(col(f.name))
                } else {
                  // otherwise don't select this column
                  Seq.empty
                }
              })

              dfMutable = dfMutable.select(selectCols: _*)
            case _ => // pass through
          }
        }
      }
      schema = dfMutable.schema
    }

    dfMutable
  }
}
