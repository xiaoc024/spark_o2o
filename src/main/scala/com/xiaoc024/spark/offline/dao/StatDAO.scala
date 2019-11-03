package com.xiaoc024.spark.offline.dao

import java.sql.{Connection, PreparedStatement}

import com.xiaoc024.spark.offline.dao.bean.{BrowseGameByCity, BrowseGameByMonth, HourTimes, PhoneModelTimes}

import scala.collection.mutable.ListBuffer

object StatDAO {


  def insertGameByMonthStat(list: ListBuffer[BrowseGameByMonth]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection
      conn.setAutoCommit(false)

      val sql = "insert into game_by_month_topn_stat(month,game_name,game_id,times) values(?,?,?,?)"
      pstmt = conn.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setInt(1, ele.month)
        pstmt.setString(2, ele.gameName)
        pstmt.setInt(3, ele.gameId)
        pstmt.setLong(4, ele.times)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn,pstmt)
    }
  }

  def insertGameByCityStat(list: ListBuffer[BrowseGameByCity]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection
      conn.setAutoCommit(false)

      val sql = "insert into game_by_city_topn_stat(city,game_id,game_name,times,ranks) values(?,?,?,?,?)"
      pstmt = conn.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.city)
        pstmt.setInt(2, ele.gameId)
        pstmt.setString(3, ele.gameName)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5,ele.rank)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn,pstmt)
    }
  }

  def insertPhoneModelTimesStat(list: ListBuffer[PhoneModelTimes]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection
      conn.setAutoCommit(false)

      val sql = "insert into game_by_phonemodel_times_stat(phonemodel,times) values(?,?)"
      pstmt = conn.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.phoneModel)
        pstmt.setLong(2, ele.times)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn,pstmt)
    }
  }

  def insertHourTimesStat(list: ListBuffer[HourTimes]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection
      conn.setAutoCommit(false)

      val sql = "insert into game_by_hour_times_stat(hour,times) values(?,?)"
      pstmt = conn.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.hour)
        pstmt.setLong(2, ele.times)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn,pstmt)
    }
  }
}
