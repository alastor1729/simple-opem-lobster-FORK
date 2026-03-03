package com.bhp.dp.visiting.utils

import com.bhp.dp.visiting.components.SourceConfig
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object SchemaMutations {

  /** Disregard the default or inferred schema types. Re-cast everything to a string. Only affects
    * leaf properties. Arrays will become arrays of strings. Structs will have leaf properties
    * recast to strings. Reading everything as string is default behavior for many file types. We
    * want to ensure that functionality for all file types.
    *
    * @param dataFrame
    *   The dataframe with the schema to modify.
    * @return
    *   A dataframe with the modified schema.
    */
  def castTypesToString(dataFrame: DataFrame): DataFrame = {

    val callback = (df: DataFrame, elem: StructField) => {
      elem.dataType match {
        // A leaf array property can still be a multi-dimensional array type. Preserve dimension when casting base element.
        case arrayType: ArrayType =>
          val (arrayDepth, _) = getArrayDepthAndElementType(arrayType)
          val castString      = createMultiDimensionalArrayType(arrayDepth, "string")
          df.withColumn(elem.name, col(elem.name).cast(castString))
        case _ =>
          df.withColumn(elem.name, col(elem.name).cast(StringType))
      }
    }

    // Dataframe must be clean to ensure special characters don't mess up cast strings.
    val cleansedDataframe = rename(dataFrame)

    val newSchema =
      traverseSchemaProperties(cleansedDataframe.schema, callback, visitLeafOnly = true)

    // Can't just set the schema. Some of the data is actually non-string. It won't match the schema and
    // break when query is evaluated. Must add casting to the query plan for underlying data to
    // be read in and cast. That's done here using the new schema, casting all top-level columns with
    // their new types (which have been recursively modified)
    var mutableDf = cleansedDataframe
    newSchema.fields.foreach { newField: StructField =>
      mutableDf = mutableDf.withColumn(newField.name, col(newField.name).cast(newField.dataType))
    }

    mutableDf
  }

  /** Perform default sanitization on all column/schema-property names.
    *
    * @param dataFrame
    *   Dataframe to sanitize.
    * @return
    *   Dataframe with sanitized column names.
    */
  def rename(dataFrame: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get

    val callback = (tempDf: DataFrame, elem: StructField) => {
      tempDf.withColumnRenamed(elem.name, cleanColumnName(elem.name))
    }

    val newSchema = traverseSchemaProperties(dataFrame.schema, callback, visitLeafOnly = false)

    // Set the new schema. Nothing about the query needs to change, only the schema names.
    spark.createDataFrame(dataFrame.rdd, newSchema)
  }

  /** Explicitly override column names.
    *
    * @param dataFrame
    *   The dataframe to modify.
    * @param columnNames
    *   An explicit list of column names to use. Must match the count of columns on the dataframe.
    * @return
    *   A dataframe using the explicitly defined column names.
    */
  def rename(dataFrame: DataFrame, columnNames: Seq[String]): DataFrame = {

    if (
      dataFrame.columns.isEmpty || (dataFrame.columns sameElements Array(
        SourceConfig.SOURCE_LINE_NUMBER_COL_NAME
      ))
    ) {
      // if there aren't any columns or the only column is the line number col, it shouldn't error because the dataframe is empty
      dataFrame
    } else {
      if (dataFrame.columns.length != columnNames.length) {
        throw new IllegalArgumentException(
          s"Column name overrides not equal to number of dataframe columns. columns=${columnNames} dataframe=${dataFrame.schema}"
        )
      }

      dataFrame.toDF(columnNames: _*)
    }
  }

  /** Traverse a schema of any structured type and visit all, or only leaf, properties. The 'visit'
    * operation can be defined externally as a callback.
    *
    * For callback, a dataframe and element are provided. The element is the property being visited.
    * The dataframe is empty, but contains the schema as modified so far. It's used in order to
    * provide all schema mutation methods.
    *
    * See tests. Handles flat schemas, structs, arrays, structs with arrays, arrays with structs,
    * arrays with arrays with structs with arrays with structs with structs.... you name it.
    *
    * @param schema
    *   The schema (struct) to traverse. Can be pulled from a dataframe with myDataframe.schema.
    * @param cb
    *   A method implementing the mutation to be done an an element. Given an empty dataframe in
    *   order to access all dataframe mutator methods, and the element to modify. Example: (df:
    *   DataFrame, elem: StructField) => { df.withColumnRenamed(elem.name, s"${elem.name}_modified"
    *   }
    * @return
    *   A modified schema/struct object. The method that calls this can override a dataframe with
    *   spark.createDataFrame(myDataframe.rdd, newSchema).
    */
  private def traverseSchemaProperties(
      schema: StructType,
      cb: (DataFrame, StructField) => DataFrame,
      visitLeafOnly: Boolean = false
  ): StructType = {
    val spark = SparkSession.getActiveSession.get

    // Temp df to manipulate otherwise immutable schema.
    var tempDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    // VISIT CHILDREN PROPERTIES (depth-first)
    schema.fields.foreach { elem =>
      elem.dataType match {
        // STRUCT
        case structType: StructType =>
          // Recurse into child properties of struct type.
          val nestedSchema = traverseSchemaProperties(structType, cb, visitLeafOnly) // recurse.
          tempDf = tempDf.withColumn(
            elem.name,
            col(elem.name).cast(nestedSchema)
          ) // apply all child changes to me

        // ARRAY
        case arrayType: ArrayType =>
          val (arrayDepth, elementType) = getArrayDepthAndElementType(arrayType)

          elementType match {
            case structType: StructType =>
              // Array contains struct type. Recurse into it.
              val nestedElementSchema =
                traverseSchemaProperties(structType, cb, visitLeafOnly) // recurse

              // Array of array is still just one property type (inner array is not a new property).
              val castString =
                createMultiDimensionalArrayType(arrayDepth, nestedElementSchema.catalogString)

              tempDf = tempDf.withColumn(
                elem.name,
                col(elem.name).cast(castString)
              ) // apply all child changes to me.

            // LEAF ARRAY PROPERTY
            // Array element has no properties to traverse. Can still be multi-dimensional (inner array is not a new property)
            case _ => if (visitLeafOnly) tempDf = cb(tempDf, elem)
          }
        // LEAF PROPERTY ex: StringType
        case _ => if (visitLeafOnly) tempDf = cb(tempDf, elem)
      }

      // VISIT THIS PROPERTY
      // All child properties have been traversed and visited. Now visit 'me'.
      if (!visitLeafOnly) tempDf = cb(tempDf, elem)
    }

    tempDf.schema
  }

  /** Get the depth of the array and the type of the base element.
    *
    * @param a
    *   The array
    * @return
    *   A tuple containing the depth of the array (1-indexed) and the base element DataType.
    */
  private def getArrayDepthAndElementType(a: ArrayType): (Int, DataType) = {
    var elementType  = a.elementType
    var elementDepth = 1 // be default, depth of 1. Element of an array.
    while (elementType.isInstanceOf[ArrayType]) {
      elementType = elementType.asInstanceOf[ArrayType].elementType
      elementDepth += 1
    }

    (elementDepth, elementType)
  }

  /** Create a type string for a multidimensional array given the depth of the array and the base
    * element type. Ex: arrayDepth 2 and elementType 'string' will produce 'array<array<string>>'
    * which can be used for schema casting or instantiating data types.
    *
    * todo If someone wants to take more time to create a regex statement that find/replaces the
    * base element type *reliably* given all possible types, this won't be needed.
    *
    * @param arrayDepth
    *   The number of nested arrays.
    * @param elementType
    *   The base element type.
    * @return
    *   A type string for the array.
    */
  private def createMultiDimensionalArrayType(arrayDepth: Int, elementType: String): String = {
    var typeString = elementType
    for (_ <- 1 to arrayDepth) {
      typeString = "array<" + typeString + ">"
    }

    typeString
  }

  def cleanColumnName(columnName: String): String = {

    var col = columnName.trim
      .toLowerCase()
      // special handling for #
      .replace("#", "_num")
      // special handling for %
      .replace("%", "pct")
      // replace anything not alpha-numeric or _ with _
      .replaceAll("[^A-Za-z0-9_]", "_")
      // replace 2 or more contiguous _ with a single _
      .replaceAll("_{2,}", "_")

    while (col.endsWith("_"))
      col = col.substring(0, col.length - 1)
    while (col.startsWith("_"))
      col = col.substring(1)
    col
  }
}
