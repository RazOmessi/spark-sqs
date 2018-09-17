package com.startapp.data.spark.ds.sqs.messages

import play.api.libs.json.{JsResult, JsValue}

trait SqsMessage

trait SqsMessageReader[T <: SqsMessage] {
  def getRecords(json: JsValue): JsValue

  def validate(jsValue: JsValue): JsResult[Seq[T]]
}
