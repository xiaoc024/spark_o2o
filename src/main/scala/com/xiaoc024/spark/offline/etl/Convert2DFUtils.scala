package com.xiaoc024.spark.offline.etl

import java.util.{Date, Locale}

import com.xiaoc024.spark.ParamsConf
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//19.167.29.40  [2018-03-04 21:10:16] (Android o,Meizu note 7)  三国志 104
object Convert2DFUtils {

  def convertByExternalDataSource(spark:SparkSession):DataFrame = {
    //使用缩写"ctxt"需要编辑spark-sql jar包中DataSourceRegister,添加com.xiaoc024.spark.offline.datasource.DefaultSource
    spark.read.format("ctxt").option("path",ParamsConf.originLogPath).load()
    //spark.read.format("com.xiaoc024.spark.offline.datasource").option("path",ParamsConf.originLogPath).load()
  }

  def convertByReflection(spark:SparkSession, rdd:RDD[String]):DataFrame = {
    import spark.implicits._

    rdd.map(line => line.split("\t")).map(attrs => BrowseLog(
                                                                  attrs(0),
                                                                  formatTime(attrs(1)),
                                                                  attrs(2).split(",")(0).filter(_!='('),
                                                                  attrs(2).split(",")(1).filter(_!=')'),
                                                                  attrs(3),
                                                                  attrs(4).toInt)).toDF
  }

  def convertByProgramming(spark:SparkSession,rdd:RDD[String]):DataFrame = {
    val rowRdd = rdd.map(line => line.split("\t")).map(attrs => Row(
                                                                          attrs(0),
                                                                          formatTime(attrs(1)),
                                                                          attrs(2).split(",")(0).filter(_!='('),
                                                                          attrs(2).split(",")(1).filter(_!=')'),
                                                                          attrs(3),
                                                                          attrs(4).toInt))
    spark.createDataFrame(rowRdd,schema)
  }

  val schema = StructType(
    Array(
      StructField("ip",StringType),
      StructField("date",StringType),
      StructField("androidVersion",StringType),
      StructField("phoneModel",StringType),
      StructField("gameName",StringType),
      StructField("gameId",IntegerType)
    ))

  def formatTime(time:String): String = {
    FastDateFormat.getInstance("yyyyMMddHHmm").format(
      new Date(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss",Locale.ENGLISH)
        .parse(time.substring(time.indexOf("[")+1, time.lastIndexOf("]"))).getTime
      ))
  }


  case class BrowseLog(ip:String,date:String,androidVersion:String,phoneModel:String,gameName:String,gameId:Int)


  def main(args: Array[String]): Unit = {
    println(formatTime("[2018-07-06 21:45:08]"))
  }
}
