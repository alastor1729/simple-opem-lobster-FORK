package com.bhp.dp.utils

object RawLoaderJobUtils {
  val isLocal: Boolean = !sys.env.contains("DATABRICKS_RUNTIME_VERSION")
  val clusterName: Option[String] = {
    val fromMap = sys.env.get("DB_CLUSTER_NAME")

    fromMap match {
      case Some(value) => if (value == null || value.trim.isEmpty) None else fromMap
      case None        => fromMap
    }
  }

  val NO_TEAMS_PREFIX: String = "[NO TEAMS]"
}
