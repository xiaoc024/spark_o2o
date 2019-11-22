package com.xiaoc024.spark

import java.util

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

/**
  *
  * 项目参数配置读取类
  *
  * 配置统一管理
  */
object ParamsConf {

  private lazy val config = ConfigFactory.load()

  val originLogPath: String = if(config.getBoolean("spark.localmode")) config.getString("ORIGINLOG.LOCAL.PATH") else config.getString("ORIGINLOG.HDFS.PATH")
  val cleanLogPath: String = if(config.getBoolean("spark.localmode")) config.getString("CLEANLOG.LOCAL.PATH") else config.getString("CLEANLOG.HDFS.PATH")
  val convert2DFWay: Int = config.getInt("rdd2df.way")
  val mysqlUrl: String = if(config.getBoolean("spark.localmode")) config.getString("MYSQL.LOCAL.URL") else config.getString("MYSQL.SERVER.URL")
  val hBaseRootDir: String = config.getString("HBASE.ROOTDIR")
  val hBaseZookeeperQuorum: String = config.getString("HBASE.ZOOKEEPER.QUORUM")
  val hBaseTableName: String = config.getString("HBASE.TABLE_NAME")
  val hBaseSavePath: String = config.getString("HBASE.SAVE_PATH")
  val useHBase: Boolean = if(!config.getBoolean("spark.localmode") && config.getBoolean("hbase.use")) true else false
  val brokers:String = config.getString("KAFKA.BROKER.LIST")
  val groupId:String = config.getString("KAFKA.GROUP.ID")
  val downloadTopic: String = config.getString("KAFKA.TOPIC.DOWNLOAD_TOPIC")
  val consumeTopic: String = config.getString("KAFKA.TOPIC.CONSUME_TOPIC")
  val topics: Array[String] = Array(downloadTopic,consumeTopic)
  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val log4jHost: String = config.getString("LOG4J.HOST")
  val log4jPort1: Int = config.getInt("LOG4J.PORT1")
  val log4jPort2: Int = config.getInt("LOG4J.PORT2")
  val analyseType: String = config.getString("streaming.analyse.type")

}
