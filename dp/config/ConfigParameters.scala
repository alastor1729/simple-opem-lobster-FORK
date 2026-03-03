package com.bhp.dp.config

case class ConfigParameters(params: Map[String, String])

object ConfigParameters {
  def apply(environment: String, lineOfBusiness: Option[String] = None): ConfigParameters =
    ConfigParameters(
      Map(
        "environment"             -> environment,
        "hiveDbPrefix"            -> (if (environment == "prod") "" else s"${environment}_"),
        "lineOfBusinessDbPrefix"  -> lineOfBusiness.map(s => s"${s}_").getOrElse(""),
        "lineOfBusinessDirectory" -> lineOfBusiness.map(s => s"$s/").getOrElse("")
      )
    )
}
