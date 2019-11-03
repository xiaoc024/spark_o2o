package com.xiaoc024.spark.offline.dao

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.xiaoc024.spark.ParamsConf

object MySQLUtils {

  def getConnection: Connection = {
    DriverManager.getConnection(ParamsConf.mysqlUrl)
  }

  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection)
  }

}
