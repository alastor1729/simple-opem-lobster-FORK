package com.bhp.dp.streaming

case class EventGridMessage(
    topic: String,
    subject: String,
    eventType: String,
    data: EventGridMessageData
)

case class EventGridMessageData(api: String)
