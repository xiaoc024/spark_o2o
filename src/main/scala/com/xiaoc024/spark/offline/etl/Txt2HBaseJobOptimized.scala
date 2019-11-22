package com.xiaoc024.spark.offline.etl

import java.util.zip.CRC32

import com.xiaoc024.spark.ParamsConf
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob}

import scala.collection.mutable.ListBuffer

//Using hbase 'bulk load' instead of 'put' to optimize
object Txt2HBaseJobOptimized {


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

    val hBaseRDD = df.rdd.flatMap(row => {
      val ip = row.getAs[String]("ip")
      val date = row.getAs[String]("date")
      val androidVersion = row.getAs[String]("androidVersion")
      val phoneModel = row.getAs[String]("phoneModel")
      val gameName = row.getAs[String]("gameName")
      val gameId = row.getAs[Int]("gameId").toString

      val rowKey: String = getRowKey(date,ip+gameId)
      val rk = Bytes.toBytes(rowKey)

      val list = new ListBuffer[((String,String),KeyValue)]()   //[(rowkey,key),KeyValue)]
      list += ( (rowKey,"ip") -> new KeyValue(rk,"o".getBytes,"ip".getBytes,ip.getBytes) )
      list += ( (rowKey,"date") -> new KeyValue(rk,"o".getBytes,"date".getBytes,date.getBytes) )
      list += ( (rowKey,"androidVersion") -> new KeyValue(rk,"o".getBytes,"androidVersion".getBytes,androidVersion.getBytes) )
      list += ( (rowKey,"phoneModel") -> new KeyValue(rk,"o".getBytes,"phoneModel".getBytes,phoneModel.getBytes) )
      list += ( (rowKey,"gameName") -> new KeyValue(rk,"o".getBytes,"gameName".getBytes,gameName.getBytes) )
      list += ( (rowKey,"gameId") -> new KeyValue(rk,"o".getBytes,"gameId".getBytes,gameId.getBytes) )

      list.toList
    }).sortByKey()//模拟HBase字典排序：rowKey + CF:column(key)
      .map(x => (new ImmutableBytesWritable(Bytes.toBytes(x._1._1)), x._2))

    val conf = new Configuration()
    conf.set("hbase.rootdir",ParamsConf.hBaseRootDir)
    conf.set("hbase.zookeeper.quorum",ParamsConf.hBaseZookeeperQuorum)
    conf.set(TableOutputFormat.OUTPUT_TABLE, createTable(conf))

    val job = NewAPIHadoopJob.getInstance(conf)
    val table = new HTable(conf, createTable(conf))
    HFileOutputFormat2.configureIncrementalLoad(job,table.getTableDescriptor,table.getRegionLocator)

    val output = ParamsConf.hBaseSavePath
    val outputPath = new Path(output)
    hBaseRDD.saveAsNewAPIHadoopFile(
      output,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job.getConfiguration
    )

    if(FileSystem.get(conf).exists(outputPath)) {
      val load = new LoadIncrementalHFiles(job.getConfiguration)
      load.doBulkLoad(outputPath, table)

      FileSystem.get(conf).delete(outputPath, true)
    }

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
