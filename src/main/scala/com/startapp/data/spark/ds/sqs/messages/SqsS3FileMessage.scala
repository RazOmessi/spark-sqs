package com.startapp.data.spark.ds.sqs.messages

import play.api.libs.json.Reads
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class SqsS3FileMessage(bucketName: String, fileName: String) extends SqsMessage {
  override def toString: String = s"$bucketName/$fileName".replace("%3D", "=")
}

object SqsS3FileMessage {
  implicit val sqsFileMessageReads: Reads[SqsS3FileMessage] = (
    (__ \ "s3" \ "bucket" \ "name").read[String] and
      (__ \ "s3" \ "object" \ "key").read[String]
    ) (SqsS3FileMessage.apply _)

  def getRecords(json: JsValue): JsValue = (json \ "Records").getOrElse(JsObject.apply(Seq.empty[(String, JsValue)]))
}

class SqsS3FileMessageReader extends SqsMessageReader[SqsS3FileMessage] {
  override def getRecords(json: JsValue): JsValue = (json \ "Records").getOrElse(JsObject.apply(Seq.empty[(String, JsValue)]))

  override def validate(jsValue: JsValue): JsResult[Seq[SqsS3FileMessage]] = jsValue.validate[Seq[SqsS3FileMessage]]
}