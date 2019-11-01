package com.xiaoc024.spark.offline

import com.xiaoc024.spark.ParamsConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Txt2ParquetJob {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                            .appName("Txt2ParquetJob")
                            .master("local[2]")
                            .getOrCreate()

    val txtRdd = spark.sparkContext.textFile(ParamsConf.originLogPath)
    val df = if(ParamsConf.convertByReflect) Rdd2DFUtils.convertByReflection(spark,txtRdd)
                                        else Rdd2DFUtils.convertByProgramming(spark,txtRdd)

//    df.printSchema()
//    df.show()

    // TODO: coalesce() and partitionBy() optimization
    df.write.format("parquet").mode(SaveMode.Overwrite).save(ParamsConf.cleanLogPath)

//    val df = spark.read.format("parquet").load(ParamsConf.cleanLogPath)
//    df.printSchema()
//    df.show()
  }


}
