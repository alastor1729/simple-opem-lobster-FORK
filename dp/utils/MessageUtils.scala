package com.bhp.dp.utils

import com.bhp.dp.streaming.EventGridMessage
import org.apache.logging.log4j.scala.Logging
import org.json4s._
import org.json4s.jackson.Serialization

import scala.util.{Failure, Success, Try}

object MessageUtils extends Logging {

  private implicit val formats: Formats = DefaultFormats

  def tryParseEventGridMessage(messageBody: String): Try[EventGridMessage] = {
    val eventGridMessage = Try(Serialization.read[EventGridMessage](messageBody))
    eventGridMessage match {
      case Failure(e) =>
        if (messageBody.startsWith("@\u0006") && messageBody.endsWith("\u0001")) {
          logger.warn(s"Failed to parse message. Attempting to parse as Track 0 message", e)
          val updatedBody = messageBody
            .replace("@\u0006string\b3http://schemas.microsoft.com/2003/10/Serialization/��", "")
            .replace("\u0001", "")
          Try(Serialization.read[EventGridMessage](updatedBody))
        } else {
          eventGridMessage
        }
      case Success(_) => eventGridMessage
    }
  }
}
