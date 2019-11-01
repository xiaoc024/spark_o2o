package com.xiaoc024.spark.offline

import com.xiaoc024.spark.ParamsConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AnalyseApp {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                            .appName("AnalyseApp")
                            .master("local[2]")
                            .getOrCreate()

    val df = spark.read.format("parquet").load(ParamsConf.cleanLogPath)

    topNBrowseGame(spark,df,3,true)
  }

  def topNBrowseGame(spark: SparkSession, df: DataFrame, month: Int, useSQL: Boolean = false): Unit = {
    import spark.implicits._

    def getMonth =  udf((time:String) => {
      time.substring(4,6)
    })
    val dfWithMonth = df.withColumn("month",getMonth($"date"))

    if(useSQL) {
      dfWithMonth.createOrReplaceTempView("browse")
      val sqlDF = spark.sql(("select \'%d月\',first(gameName) as gameName,gameId,count(*) as times from browse " +
                              "where month=%d group by gameId").format(month,month))
      sqlDF.show()
    }
    else {
      val topNDF = dfWithMonth.filter($"month" === month)
        .groupBy("gameId")
        .agg(first("gameName").as("gameName"),count("gameId").as("times"),lit(month+"月"))
        .orderBy($"times".desc)

      topNDF.show()
    }
  }

}
