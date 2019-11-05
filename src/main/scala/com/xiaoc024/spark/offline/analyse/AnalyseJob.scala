package com.xiaoc024.spark.offline.analyse

import com.xiaoc024.spark.offline.analyse.AnalyseJob.DFType.DFType
import com.xiaoc024.spark.offline.dao.StatDAO
import com.xiaoc024.spark.offline.dao.bean.{BrowseGameByCity, BrowseGameByMonth, HourTimes, PhoneModelTimes}
import com.xiaoc024.spark.{IpUtils, ParamsConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, first, row_number, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object AnalyseJob {


  def main(args: Array[String]): Unit = {
    analyse()
  }

  def analyse(): Unit ={
    val spark = SparkSession.builder()
      .appName("AnalyseApp")
      .master("local[2]")
      .getOrCreate()

    topNBrowseGameByMonth(spark,getDF(spark,DFType.TOPN_BROWSE_GAME_BY_MONTH),3)
    top3BrowseGameByCity(spark,getDF(spark,DFType.TOP3_BROWSE_GAME_BY_CITY))
    topNPhoneModel(spark,getDF(spark,DFType.TOPN_PHONE_MODEL),useSQL = true)
    analyseByTime(spark,getDF(spark,DFType.ANALYSE_BY_TIME),useSQL = true)

    spark.stop()
  }

  //按月统计topN访问的游戏
  def topNBrowseGameByMonth(spark: SparkSession, df: DataFrame, month: Int, useSQL: Boolean = false): Unit = {
    import spark.implicits._

    def getMonth =  udf((time:String) => {
      time.substring(4,6).toInt
    })
    val dfWithMonth = df.withColumn("month",getMonth($"date"))
    var topNDF: DataFrame = null

    if(useSQL) {
      dfWithMonth.createOrReplaceTempView("browse_by_month")
      topNDF = spark.sql(("select first(month) as month,first(gameName) as gameName,gameId,count(*) as times from browse_by_month " +
                              "where month=%d group by gameId order by times desc").format(month))
      topNDF.show()
    }
    else {
      topNDF = dfWithMonth.filter($"month" === month)
        .groupBy("gameId")
        .agg(first("month").as("month"),first("gameName").as("gameName"),count("gameId").as("times"))
        .orderBy($"times".desc)

      topNDF.show()
    }

    topNDF.foreachPartition(onePartition => {
      val list = new ListBuffer[BrowseGameByMonth]
      try {
        onePartition.foreach(row => {
          val month = row.getAs[Int]("month")
          val gameName = row.getAs[String]("gameName")
          val gameId = row.getAs[Int]("gameId")
          val times = row.getAs[Long]("times")
          list.append(BrowseGameByMonth(month, gameName, gameId, times))
        })
        StatDAO.insertGameByMonthStat(list)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    })
  }

  //按城市统计的topN访问的游戏
  def top3BrowseGameByCity(spark: SparkSession, df: DataFrame, useSQL: Boolean = false): Unit = {
    import spark.implicits._

    def getCity = udf((ip: String) => {
      IpUtils.getCity(ip)
    })
    val dfWithCity = df.withColumn("city",getCity($"ip"))
    var top3DF: DataFrame = null

    if(useSQL) {
      dfWithCity.createOrReplaceTempView("browse_by_city")
      top3DF = spark.sql("select city,gameId,gameName,times,row_number() over (partition by city order by times desc) as rank from " +
                                   "(select city,gameId,gameName,count(*) as times from browse_by_city group by city,gameId,gameName)")

      top3DF.show(50)
    }
    else {
      val topNDF = dfWithCity.groupBy("city","gameId")
                         .agg(first("gameName").as("gameName"), count("gameId").as("times"))
                         .orderBy($"times".desc)

      top3DF = topNDF.select($"city",$"gameId",$"gameName",$"times",
        row_number().over(Window.partitionBy($"city").orderBy($"times".desc)).as("rank"))
        .filter($"rank" <= 3)

      top3DF.show(50)
    }

    top3DF.foreachPartition(onePartition => {
      try {
        val list = new ListBuffer[BrowseGameByCity]
        onePartition.foreach(row => {
          val city = row.getAs[String]("city")
          val gameId = row.getAs[Int]("gameId")
          val gameName = row.getAs[String]("gameName")
          val times = row.getAs[Long]("times")
          val rank = row.getAs[Int]("rank")
          list.append(BrowseGameByCity(city,gameId,gameName,times,rank))
        })
        StatDAO.insertGameByCityStat(list)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    })
  }

  //打开游戏中心的topN魅族手机的型号
  def topNPhoneModel(spark: SparkSession, df: DataFrame, useSQL: Boolean = false): Unit = {
    import spark.implicits._

    var topNDF: DataFrame = null
    if(useSQL) {
      df.createOrReplaceTempView("browse")
      topNDF = spark.sql("select phoneModel,count(*) as times from browse group by phoneModel order by times desc")

      topNDF.show()
    }
    else {
      topNDF = df.groupBy("phoneModel")
                 .agg(count("gameId").as("times"))
                 .orderBy($"times".desc)

      topNDF.show()
    }

    topNDF.foreachPartition(onePartition => {
      try {
        val list = new ListBuffer[PhoneModelTimes]
        onePartition.foreach(row => {
          val phoneModel = row.getAs[String]("phoneModel")
          val times = row.getAs[Long]("times")
          list.append(PhoneModelTimes(phoneModel,times))
        })
        StatDAO.insertPhoneModelTimesStat(list)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    })
  }

  //按时段统计游戏中心的访问量
  def analyseByTime(spark: SparkSession, df: DataFrame, useSQL: Boolean = false): Unit = {
    import spark.implicits._

    def getHour = udf((time: String) => {
      time.substring(8,10)
    })
    val dfWithHour = df.withColumn("hour",getHour($"date"))
    var timeDF: DataFrame = null

    if(useSQL) {
      dfWithHour.createOrReplaceTempView("browse_by_hour")
      timeDF = spark.sql("select hour,count(*) as times from browse_by_hour group by hour order by hour")
      timeDF.show(24)
    }
    else {
      timeDF = dfWithHour.groupBy("hour")
          .agg(count("gameId").as("times"))
          .orderBy("hour")

      timeDF.show(24)
    }

    timeDF.foreachPartition(onePartition => {
      try {
        val list = new ListBuffer[HourTimes]
        onePartition.foreach(row => {
          val hour = row.getAs[String]("hour")
          val times = row.getAs[Long]("times")
          list.append(HourTimes(hour,times))
        })
        StatDAO.insertHourTimesStat(list)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    })

  }

  def getDF(spark: SparkSession,dfType: DFType = null): DataFrame = {
    import spark.implicits._

    if(ParamsConf.useHBase) {
      val conf = new Configuration()
      conf.set("hbase.rootdir", ParamsConf.hBaseRootDir)
      conf.set("hbase.zookeeper.quorum", ParamsConf.hBaseZookeeperQuorum)
      conf.set(TableInputFormat.INPUT_TABLE, ParamsConf.hBaseTableName)

      val scan = new Scan()
      scan.addFamily(Bytes.toBytes("o"))

      dfType match {
        case DFType.TOPN_BROWSE_GAME_BY_MONTH =>
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("date"))
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("gameName"))
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("gameId"))
        case DFType.TOP3_BROWSE_GAME_BY_CITY =>
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("ip"))
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("gameName"))
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("gameId"))
        case DFType.TOPN_PHONE_MODEL =>
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("phoneModel"))
        case DFType.ANALYSE_BY_TIME =>
          scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("date"))
      }
      conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

      val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )

      var df: DataFrame = null
      dfType match {
        case DFType.TOPN_BROWSE_GAME_BY_MONTH =>
          df = hbaseRDD.map(x => {
            val date = Bytes.toString(x._2.getValue("o".getBytes, "date".getBytes))
            val gameName = Bytes.toString(x._2.getValue("o".getBytes, "gameName".getBytes))
            val gameId = Integer.valueOf(Bytes.toString(x._2.getValue("o".getBytes, "gameId".getBytes)))

            GameByMonth(date,gameName,gameId)
          }).toDF()

        case DFType.TOP3_BROWSE_GAME_BY_CITY =>
          df = hbaseRDD.map(x => {
            val ip = Bytes.toString(x._2.getValue("o".getBytes, "ip".getBytes))
            val gameName = Bytes.toString(x._2.getValue("o".getBytes, "gameName".getBytes))
            val gameId = Integer.valueOf(Bytes.toString(x._2.getValue("o".getBytes, "gameId".getBytes)))

            GameByCity(ip,gameName,gameId)
          }).toDF()

        case DFType.TOPN_PHONE_MODEL =>
          df = hbaseRDD.map(x => {
            val phoneModel = Bytes.toString(x._2.getValue("o".getBytes, "phoneModel".getBytes))

            PhoneModel(phoneModel)
          }).toDF()

        case DFType.ANALYSE_BY_TIME =>
          df = hbaseRDD.map(x => {
            val date = Bytes.toString(x._2.getValue("o".getBytes, "date".getBytes))

            Time(date)
          }).toDF()
      }
      df
    }
    else spark.read.format("parquet").load(ParamsConf.cleanLogPath)
  }

  object DFType extends Enumeration {
    type DFType = Value
    val TOPN_BROWSE_GAME_BY_MONTH,TOP3_BROWSE_GAME_BY_CITY,TOPN_PHONE_MODEL,ANALYSE_BY_TIME = Value
  }
  case class GameByMonth(date: String,gameName: String,gameId: Int)
  case class GameByCity(ip: String,gameName: String,gameId: Int)
  case class PhoneModel(phoneModel: String)
  case class Time(date: String)
}
