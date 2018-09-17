package com.startapp.data.spark.ds.sqs.messages

import com.amazonaws.services.sqs.model.Message
import com.startapp.data.spark.ds.sqs.client.SqsClient

case class SqsMessagesResponse[T <: SqsMessage](messages: List[Message], results: List[T]) {
  def deleteAllMessages(sqsClient: SqsClient): Unit = sqsClient.deleteMessages(messages: _*)
}