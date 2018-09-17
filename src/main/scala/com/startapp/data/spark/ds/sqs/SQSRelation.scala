package com.startapp.data.spark.ds.sqs

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class SQSRelation(
    sc: SQLContext,
    format: String,
    files: Seq[String],
    innerParameters: Map[String,String],
    customSchema: StructType) extends BaseRelation with TableScan{

  private val logger = Logger.getLogger(classOf[SQSRelation])

  val df: DataFrame = read()
  private def read(): DataFrame = {
    var dataframeReader = sqlContext.read
    if (customSchema != null) {
      dataframeReader = dataframeReader.schema(customSchema)
    }

    dataframeReader
      .format(format)
      .options(innerParameters)
      .load(files : _*)
  }

  override def schema: StructType = {
    df.schema
  }

  override def buildScan(): RDD[Row] = {
    df.rdd
  }

  override def sqlContext: SQLContext = sc
}
