package com.bhp.dp.visiting.configurations

import com.bhp.dp.events.DataZone._
import com.bhp.dp.visiting.components.{SourceConfig, TargetConfig}

case class LoadDefinition(
    source: SourceConfig,
    target: TargetConfig,
    configName: Option[String] = None,
    path: Option[String] = None,
    extension: Option[String] = None,
    containerName: Option[String] = None,
    dataZone: Option[DataZone] = None,
    notifyOnSuccess: Option[Boolean] = None,
    storageAccount: Option[String] = None,
    definitionId: Option[String] = None,
    partitionKey: Option[String] = None,
    id: Option[String] = None
)
