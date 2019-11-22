package com.xiaoc024.spark.offline.etl

import com.xiaoc024.spark.ParamsConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Txt2ParquetJob {


  def main(args: Array[String]): Unit = {
    etl()
  }

  def etl(): Unit = {
    val spark = SparkSession.builder()
      .appName("Txt2ParquetJob")
      .master("local[2]")
      .getOrCreate()

    val txtRdd = spark.sparkContext.textFile(ParamsConf.originLogPath)
    val df = ParamsConf.convert2DFWay match {
      case 0 => Convert2DFUtils.convertByExternalDataSource(spark)
      case 1 => Convert2DFUtils.convertByReflection(spark,txtRdd)
      case 2 => Convert2DFUtils.convertByProgramming(spark,txtRdd)
    }

    //    df.printSchema()
    //    df.show()

    df.write.format("parquet").mode(SaveMode.Overwrite).save(ParamsConf.cleanLogPath)

    //    val df = spark.read.format("parquet").load(ParamsConf.cleanLogPath)
    //    df.printSchema()
    //    df.show()

    spark.stop()
  }


}
