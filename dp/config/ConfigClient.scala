package com.bhp.dp.config

trait ConfigClient {
  def getFile(url: String): (String, String)
}
