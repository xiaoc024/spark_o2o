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

  val originLogPath: String = if(config.getBoolean("spark.localmode")) config.getString("ORIGINLOG.LOCAL.PATH") else config.getString("ORIGINLOG.HDFS.PATH")
  val cleanLogPath: String = if(config.getBoolean("spark.localmode")) config.getString("CLEANLOG.LOCAL.PATH") else config.getString("CLEANLOG.HDFS.PATH")
  val convertByReflect: Boolean = if(config.getInt("rdd2df.way") == 0) true else false
  val mysqlUrl: String = if(config.getBoolean("spark.localmode")) config.getString("MYSQL.LOCAL.URL") else config.getString("MYSQL.SERVER.URL")

}
