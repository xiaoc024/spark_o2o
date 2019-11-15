package com.xiaoc024.spark.online

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiaoc024.spark.ParamsConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object StreamingApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("StreamingApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(".")

    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](ParamsConf.topics,ParamsConf.kafkaParams)
    )

    val downloadDStream = stream.filter(_.topic().equals(ParamsConf.downloadTopic))
                                 .map(x=>JSON.parseObject(x.value()))
                                 .map(x => {
                                   val time = x.getString("time")
                                   val day = time.substring(0,8)
                                   val hour = time.substring(8,10)
                                   val min = time.substring(10,12)
                                   val userId = x.getIntValue("user_id")

                                   (day,hour,min,userId)
                                 })

    val consumeDStream = stream.filter(_.topic().equals(ParamsConf.consumeTopic))
                                .map(x=>JSON.parseObject(x.value()))
                                .map(x => {
                                   val flag  = x.getString("flag")
                                   val time = x.getString("time")
                                   val day = time.substring(0,8)
                                   val hour = time.substring(8,10)
                                   val min = time.substring(10,12)
                                   val fee = x.getLongValue("fee")
                                   val success = if(flag=="1") 1L else 0L
                                   val userId = x.getIntValue("user_id")

                                   (day,hour,min,(1L,success,fee),userId)
                                })

    ParamsConf.analyseType match {
      case "D" =>
        //按天统计
        downloadDStream.map(x=>(x._1,1)).updateStateByKey(updateCount).print()
        consumeDStream.map(x=>(x._1,x._4)).updateStateByKey(updateConsume).print()

      case "H" =>
        //按小时统计
        downloadDStream.map(x=>(x._1,1)).updateStateByKey(updateCount).print()
        consumeDStream.map(x=>(x._2,x._4)).updateStateByKey(updateConsume).print()

      case "M" =>
        //按分钟统计
        downloadDStream.map(x=>(x._1,1)).updateStateByKey(updateCount).print()
        consumeDStream.map(x=>(x._3,x._4)).updateStateByKey(updateConsume).print()
    }

    //每隔5s统计前10s的数据
    downloadDStream.map(_=>1).reduceByWindow((a:Int, b:Int)=>{a+b},Seconds(10),Seconds(5)).print()
    consumeDStream.map(x=>x._4).reduceByWindow((t1:(Long,Long,Long),t2:(Long,Long,Long)) => {(t1._1+t2._1, t1._2+t2._2, t1._3+t2._3)},Seconds(10),Seconds(5)).print()

    //黑名单
    val blacksRDD = ssc.sparkContext.parallelize({0 until 9000}).map(x => (x,true))

    val filteredBlacksDownloadDStream = downloadDStream.map(x=>(x._4,(x._1,x._2,x._3)))
                                                       .transform(rdd => {
                                                         rdd.leftOuterJoin(blacksRDD)
                                                            .filter(x=>x._2._2.getOrElse(false) != true)
                                                            .map(x=>(x._2._1,x._1))
                                                       })

    val filteredBlacksConsumeDStream = consumeDStream.map(x=>(x._5,(x._1,x._2,x._3,x._4)))
                                                     .transform(rdd => {
                                                       rdd.leftOuterJoin(blacksRDD)
                                                          .filter(x=>x._2._2.getOrElse(false) != true)
                                                          .map(x=>(x._2._1,x._1))
                                                     })

    filteredBlacksDownloadDStream.print()
    filteredBlacksConsumeDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateCount(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

  def updateConsume(currentValues: Seq[(Long,Long,Long)], preValues: Option[(Long,Long,Long)]): Option[(Long,Long,Long)] = {
    val current = currentValues.reduce((t1,t2) => {
      (t1._1+t2._1, t1._2+t2._2, t1._3+t2._3)
    })
    val pre = preValues.getOrElse((1L,0L,0L))

    Some((current._1+pre._1, current._2+pre._2, current._3+pre._3))
  }

}
