package com.xiaoc024.spark.offline.etl

import java.util.zip.CRC32

import com.xiaoc024.spark.ParamsConf
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.SparkSession

object Txt2HBaseJob {


  def main(args: Array[String]): Unit = {
    etl()
  }

  def etl(): Unit = {
    val spark = SparkSession.builder()
      .appName("Txt2HBaseJob")
      .master("local[2]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val txtRdd = spark.sparkContext.textFile(ParamsConf.originLogPath)
    val df = ParamsConf.convert2DFWay match {
      case 0 => Convert2DFUtils.convertByExternalDataSource(spark,ParamsConf.originLogPath)
      case 1 => Convert2DFUtils.convertByReflection(spark,txtRdd)
      case 2 => Convert2DFUtils.convertByProgramming(spark,txtRdd)
    }

    val hBaseRDD = df.rdd.map(row => {
      val ip = row.getAs[String]("ip")
      val date = row.getAs[String]("date")
      val androidVersion = row.getAs[String]("androidVersion")
      val phoneModel = row.getAs[String]("phoneModel")
      val gameName = row.getAs[String]("gameName")
      val gameId = row.getAs[Int]("gameId").toString

      val rowKey: String = getRowKey(date,ip+gameId)

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("ip"), Bytes.toBytes(ip))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("date"), Bytes.toBytes(date))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("androidVersion"), Bytes.toBytes(androidVersion))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("phoneModel"), Bytes.toBytes(phoneModel))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("gameName"), Bytes.toBytes(gameName))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("gameId"), Bytes.toBytes(gameId))

      (new ImmutableBytesWritable(rowKey.getBytes), put)
    })

    val conf = new Configuration()
    conf.set("hbase.rootdir",ParamsConf.hBaseRootDir)
    conf.set("hbase.zookeeper.quorum",ParamsConf.hBaseZookeeperQuorum)
    conf.set(TableOutputFormat.OUTPUT_TABLE, createTable(conf))

    hBaseRDD.saveAsNewAPIHadoopFile(
      ParamsConf.hBaseSavePath,
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

    spark.stop()
  }


  def getRowKey(date: String, info: String): String = {
    val builder = new StringBuilder(date)
    builder.append("_")

    val crc32 = new CRC32()
    crc32.reset()
    if(StringUtils.isNotEmpty(info)){
      crc32.update(Bytes.toBytes(info))
    }

    builder.append(crc32.getValue).toString()
  }

  def createTable(conf:Configuration): String ={
    val table = ParamsConf.hBaseTableName

    var connection:Connection = null
    var admin:Admin = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin

      val tableName = TableName.valueOf(table)
      if(admin.tableExists(tableName)) {
        return table
      }

      val tableDesc = new HTableDescriptor(TableName.valueOf(table))
      val columnDesc = new HColumnDescriptor("o")
      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if(null != admin) {
        admin.close()
      }

      if(null != connection) {
        connection.close()
      }
    }

    table
  }


}
