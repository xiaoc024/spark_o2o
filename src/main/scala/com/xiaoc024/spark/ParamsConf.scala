package com.xiaoc024.spark

import com.typesafe.config.ConfigFactory

/**
  *
  * 项目参数配置读取类
  *
  * 配置统一管理
  */
object ParamsConf {

  private lazy val config = ConfigFactory.load()

  val originLogPath: String = if(config.getBoolean("spark.localmode")) config.getString("originLog.local.path") else config.getString("originLog.hdfs.path")
  val cleanLogPath: String = if(config.getBoolean("spark.localmode")) config.getString("cleanLog.local.path") else config.getString("cleanLog.hdfs.path")
  val convertByReflect: Boolean = if(config.getInt("rdd2df.way") == 0) true else false

}
