package com.bhp.dp.visiting.utils

import org.apache.spark.sql.SaveMode
import org.json4s._

class SaveModeEnumSerializer extends Serializer[SaveMode] {
  private val IntervalClass = classOf[SaveMode]

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), SaveMode] = {
    case (TypeInfo(IntervalClass, _), json) =>
      json match {
        case JString(y) => SaveMode.valueOf(y)
        case x          => throw new MappingException("Can't convert " + x + " to SaveMode")
      }
  }

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = { case x: SaveMode =>
    JString(x.toString)
  }
}
