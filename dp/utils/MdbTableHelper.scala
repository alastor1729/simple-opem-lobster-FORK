package com.bhp.dp.utils

import java.sql.{Connection, DriverManager, ResultSet}

/** run this to list all tables available in an mdb file for ingestion. Make sure to update the path
  * before running,
  */
object MdbTableHelper {
  def main(args: Array[String]): Unit = {
    val mdbFilePath =
      """C:\Users\SpencerKlawa_paxybel\Desktop\snapshot_date=2021_08_24_00_00_00\test.mdb"""
    val jdbcUrl = s"jdbc:ucanaccess://$mdbFilePath"

    var connection: Connection = null

    try {
      // Load the JDBC driver
      Class.forName("net.ucanaccess.jdbc.UcanaccessDriver")

      // Establish the connection
      connection = DriverManager.getConnection(jdbcUrl)

      // Get the metadata
      val metaData = connection.getMetaData

      // Get table names
      val tableSet: ResultSet = metaData.getTables(null, null, "%", Array("TABLE"))
      // val catalogSet: ResultSet = metaData.getCatalogs()
      // val schemaSet: ResultSet = metaData.getSchemas()

      println("List of tables:")
      while (tableSet.next()) {
        println(tableSet.getString("TABLE_NAME"))
      }

//      println("List of schemas:")
//      while (schemaSet.next()) {
//        println(schemaSet.getString("TABLE_NAME"))
//      }

//      println("List of catalogs:")
//      while (catalogSet.next()) {
//        println(catalogSet.getString("TABLE_NAME"))
//      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null && !connection.isClosed) {
        connection.close()
      }
    }
  }
}
