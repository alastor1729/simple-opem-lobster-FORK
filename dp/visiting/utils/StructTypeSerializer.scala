package com.bhp.dp.visiting.utils

import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

class StructTypeSerializer extends Serializer[StructType] {
  private val IntervalClass = classOf[StructType]

  override def deserialize(implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), StructType] = {
    case (TypeInfo(IntervalClass, _), json) => {
      if (json != JNull) {
        val jsonTopStruct = ("type" -> "struct") ~ ("fields" -> json)

        val jsonAddedNullable = addNullable(jsonTopStruct)

        DataType.fromJson(compact(render(jsonAddedNullable))).asInstanceOf[StructType]
      } else {
        null
      }
    }
  }

  private def addNullable(jValue: JValue): JValue = {
    jValue match {
      case JsonAST.JObject(obj) =>
        if (obj.values.isEmpty) {
          obj
        } else {
          val isStructOrArray = Seq("struct", "array", "map").contains(obj.values("type"))
          val hasNullable     = obj.values.contains("nullable")

          // all Map/Transform functions are recursive, which is not what we want, so we do the mapping ourselves
          val mappedObject = JObject(obj.map { case (key, value) =>
            (key, addNullable(value))
          })

          if (isStructOrArray || hasNullable) mappedObject else mappedObject ~ ("nullable" -> true)
        }
      case JsonAST.JArray(arr) => JArray(arr.map(addNullable))
      case _                   => jValue
    }
  }

  override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case s: StructType =>
      val jValue = parse(s.json).asInstanceOf[JObject]
      jValue
        .findField { case (key, value) =>
          key == "fields"
        }
        .get
        ._2
  }
}
