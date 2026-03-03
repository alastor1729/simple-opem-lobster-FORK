package com.bhp.dp.utils

import java.io.InputStream
import java.util.Properties

class PropertiesReader(propertyFileName: String) {
  val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream(propertyFileName)
  val properties               = new Properties
  properties.load(inputStream)

  def get(propertyName: String): String = properties.getProperty(propertyName)
}
