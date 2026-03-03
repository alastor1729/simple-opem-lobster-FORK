package com.bhp.dp.config

object ConfigHelper {
  private val blobs               = "blobs"
  private val containerPrecursor  = "containers/"
  private val containerPostcursor = "/blobs"
  private val abfssPrecursor =
    "abfss://datalake-engineering@neudatalakeprsa01.dfs.core.windows.net"
  private val abfssIntegrationPrecursor =
    "abfss://datalake-integration@neudatalakeprsa01.dfs.core.windows.net"

  def getLobMountString(lineOfBusiness: Option[String]): String =
    lineOfBusiness.map(s => s"$s/").getOrElse("")

  def cleanBlobSubject(subject: String): String =
    subject.substring(subject.indexOf(blobs) + blobs.length)

  def cleanAbfssSubject(subject: String): String = {
    if (subject.contains(abfssPrecursor)) {
      subject.substring(subject.indexOf(abfssPrecursor) + abfssPrecursor.length)
    } else if (subject.contains(abfssIntegrationPrecursor)) {
      subject.substring(
        subject.indexOf(abfssIntegrationPrecursor) + abfssIntegrationPrecursor.length
      )
    } else {
      subject
    }
  }

  // gets container from subjects like /blobServices/default/containers/CONTAINER/blobs/folder/folder/folder/file.file
  def getContainerBlobbed(subject: String): Option[String] = {
    val indexOfStart = subject.indexOf(containerPrecursor)
    val indexOfEnd   = subject.indexOf(containerPostcursor)

    if (indexOfStart == -1 || indexOfEnd == -1 || indexOfEnd - indexOfStart <= 0) {
      None
    } else {
      Some(subject.substring(indexOfStart + containerPrecursor.length, indexOfEnd))
    }
  }

  // gets container from subjects like abfss://CONTAINER@neudatalakeprsa01.dfs.core.windows.net/folder/folder/folder/file.file
  def getContainerAbfss(subject: String): Option[String] = {
    val precursorString = "abfss://"
    val indexOfStart    = subject.indexOf(precursorString)
    val indexOfEnd      = subject.indexOf("@")

    if (indexOfStart == -1 || indexOfEnd == -1 || indexOfEnd - indexOfStart <= 0) {
      None
    } else {
      Some(subject.substring(indexOfStart + precursorString.length, indexOfEnd))
    }
  }
}
