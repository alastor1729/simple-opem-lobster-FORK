package com.bhp.dp.events

import sttp.client3.UriContext
import sttp.model.Uri

object DataZone extends Enumeration {
  type DataZone = Value
  protected case class DataZoneDetails(i: Int, name: String, msTeamsWebhook: Uri)
      extends super.Val(i, name)

  import scala.language.implicitConversions
  implicit def valueToDataZoneDetails(x: Value): DataZoneDetails = x.asInstanceOf[DataZoneDetails]

  val INFRA: DataZoneDetails = DataZoneDetails(
    1,
    "Infra",
    uri"https://neuehealth.webhook.office.com/webhookb2/6deea065-9afd-47b7-a576-cbacc17fcb55@00770399-ec2e-4bba-ba13-601b2ed23bdf/IncomingWebhook/16846bd2ef334e78bd03c9e71cfd2c72/75a0fd62-cb52-4c50-990a-cf17a94f7c2e"
  )
  val CLINICAL: DataZoneDetails = DataZoneDetails(
    2,
    "Clinical",
    uri"https://neuehealth.webhook.office.com/webhookb2/6deea065-9afd-47b7-a576-cbacc17fcb55@00770399-ec2e-4bba-ba13-601b2ed23bdf/IncomingWebhook/16846bd2ef334e78bd03c9e71cfd2c72/75a0fd62-cb52-4c50-990a-cf17a94f7c2e"
  )
  val PROVIDER: DataZoneDetails = DataZoneDetails(
    3,
    "Provider",
    uri"https://neuehealth.webhook.office.com/webhookb2/6deea065-9afd-47b7-a576-cbacc17fcb55@00770399-ec2e-4bba-ba13-601b2ed23bdf/IncomingWebhook/16846bd2ef334e78bd03c9e71cfd2c72/75a0fd62-cb52-4c50-990a-cf17a94f7c2e"
  )
  val CLAIMS: DataZoneDetails = DataZoneDetails(
    4,
    "Claims",
    uri"https://neuehealth.webhook.office.com/webhookb2/6deea065-9afd-47b7-a576-cbacc17fcb55@00770399-ec2e-4bba-ba13-601b2ed23bdf/IncomingWebhook/16846bd2ef334e78bd03c9e71cfd2c72/75a0fd62-cb52-4c50-990a-cf17a94f7c2e"
  )
  val MEMBER: DataZoneDetails = DataZoneDetails(
    5,
    "Member",
    uri"https://neuehealth.webhook.office.com/webhookb2/6deea065-9afd-47b7-a576-cbacc17fcb55@00770399-ec2e-4bba-ba13-601b2ed23bdf/IncomingWebhook/16846bd2ef334e78bd03c9e71cfd2c72/75a0fd62-cb52-4c50-990a-cf17a94f7c2e"
  )
  val NEUEHEALTH: DataZoneDetails = DataZoneDetails(
    6,
    "NeueHealth",
    uri"https://neuehealth.webhook.office.com/webhookb2/6deea065-9afd-47b7-a576-cbacc17fcb55@00770399-ec2e-4bba-ba13-601b2ed23bdf/IncomingWebhook/16846bd2ef334e78bd03c9e71cfd2c72/75a0fd62-cb52-4c50-990a-cf17a94f7c2e"
  )
}

import org.json4s._
import DataZone._

class DataZoneSerializer extends Serializer[DataZone] {
  private val IntervalClass = classOf[DataZone]

  override def deserialize(implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), DataZone] = { case (TypeInfo(IntervalClass, _), json) =>
    json match {
      case JsonAST.JString(s) =>
        try {
          DataZone.withName(s)
        } catch {
          case e: NoSuchElementException =>
            throw MappingException(
              s"Unknown DataZone '$s'. Please check that this DataZone is represented in the com.bhp.dp.events.DataZone enumeration.",
              e
            )
        }
      case x => throw new MappingException(s"Can't convert '$x' to DataZone")
    }
  }

  override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case x: DataZone => JString(x.toString)
  }
}
