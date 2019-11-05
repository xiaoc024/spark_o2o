package com.xiaoc024.spark.offline

import com.xiaoc024.spark.ParamsConf
import com.xiaoc024.spark.offline.analyse.AnalyseJob
import com.xiaoc024.spark.offline.etl.{Txt2HBaseJob, Txt2ParquetJob}

object OfflineApp {

  def main(args: Array[String]): Unit = {
    //ETL
    if(ParamsConf.useHBase) {
      Txt2HBaseJob.etl()
    }
    else {
      Txt2ParquetJob.etl()
    }

    //analyse
    AnalyseJob.analyse()
  }

}
