package com.bhp.dp.events

import com.bhp.dp.visiting.components.WriteInfo

object MessageStatus extends Enumeration {
  type MessageStatus = Value
  val STARTED, SUCCESS, FAILED = Value
}

import com.bhp.dp.events.MessageStatus._

sealed trait EventMessage {
  val sourcePath: String
  val configurationName: String
  val ingestId: String
  val correlationId: String
  val status: MessageStatus
}

case class StartMessage(
    sourcePath: String,
    configurationName: String,
    ingestId: String,
    correlationId: String,
    status: MessageStatus = MessageStatus.STARTED
) extends EventMessage

case class SuccessMessage(
    sourcePath: String,
    configurationName: String,
    ingestId: String,
    correlationId: String,
    snapshotDate: String,
    ingestedDetails: Seq[WriteInfo],
    status: MessageStatus = MessageStatus.SUCCESS
) extends EventMessage

case class FailMessage(
    sourcePath: String,
    configurationName: String,
    ingestId: String,
    correlationId: String,
    status: MessageStatus = MessageStatus.FAILED
) extends EventMessage
