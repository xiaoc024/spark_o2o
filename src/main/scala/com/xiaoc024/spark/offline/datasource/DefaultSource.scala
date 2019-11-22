package com.xiaoc024.spark.offline.datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class DefaultSource extends RelationProvider with DataSourceRegister{

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path")

    path match {
      case Some(p) => new CTxtRelation(sqlContext, p)
    }
  }

  override def shortName(): String = "ctxt"
}