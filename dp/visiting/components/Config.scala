package com.bhp.dp.visiting.components

trait Config {
  def tokens: Map[String, String]
  def configurationSummary: String
}
