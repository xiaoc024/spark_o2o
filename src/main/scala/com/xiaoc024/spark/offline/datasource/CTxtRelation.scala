package com.xiaoc024.spark.offline.datasource

import com.xiaoc024.spark.offline.etl.Convert2DFUtils.formatTime
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

class CTxtRelation(override val sqlContext:SQLContext,
                   path:String,
                   userSchema:StructType = null)
  extends BaseRelation
    with TableScan
    with InsertableRelation
    with Logging{

//19.167.29.40   [2018-03-04 21:10:16]   (Android o,Meizu note 7)    三国志 104
  override def schema: StructType = {
    if(userSchema != null){
      userSchema
    }else{
      StructType(
        Array(
          StructField("ip",StringType),
          StructField("date",StringType),
          StructField("androidVersion",StringType),
          StructField("phoneModel",StringType),
          StructField("gameName",StringType),
          StructField("gameId",IntegerType)
        ))
    }
  }

  override def buildScan(): RDD[Row] = {
    val rdd = sqlContext.sparkContext.textFile(path)
    rdd.map(line => line.split("\t")).map(attrs => Row(
      attrs(0),
      formatTime(attrs(1)),
      attrs(2).split(",")(0).filter(_!='('),
      attrs(2).split(",")(1).filter(_!=')'),
      attrs(3),
      attrs(4).toInt))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .save(path)
  }
}
