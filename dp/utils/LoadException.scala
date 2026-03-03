package com.bhp.dp.utils

import com.bhp.dp.visiting.configurations.LoadDefinition

class LoadException(
    val loadDefinition: LoadDefinition,
    val subject: String,
    val correlationId: String,
    val ingestId: String,
    cause: Throwable
) extends Exception(cause) {}
