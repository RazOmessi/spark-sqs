package com.startapp.data.spark.ds.sqs.client

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.model.{Message, ReceiveMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClient}
import com.startapp.data.spark.ds.sqs.messages.{SqsMessage, SqsMessagesResponse}
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider
import play.api.libs.json.Json.parse
import play.api.libs.json.{JsResult, JsValue}

import scala.collection.JavaConversions._

class SqsClient private (sqs: AmazonSQS, queueUrl: String, minMessages: Int, maxMessages : Int) {
  private def _getMessages[T <: SqsMessage](messages: List[Message], results: List[T], validator: JsValue => JsResult[Seq[T]], maxRetries : Int) : SqsMessagesResponse[T] = {
    if (maxRetries < 0 || results.length == maxMessages) {

      if(results.length < minMessages){
        sys.error(s"Queue does not contains enough messages.")
      }

      return SqsMessagesResponse(messages, results)
    }

    val maxReadMessages = Math.min(maxMessages - results.length, 10)
    val newMessages = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(maxReadMessages)).getMessages.toList
    val newResults = newMessages.map(_.getBody).map(parse).map(validator)
      .map(_.fold(_ => None, msg => Some(msg)))
      .filter(_.isDefined).flatMap(_.get)

    _getMessages((messages ++ newMessages).distinct, (results ++ newResults).distinct, validator, maxRetries - 1)
  }

  def getMessages[T <: SqsMessage](validator: JsValue => JsResult[Seq[T]]) : SqsMessagesResponse[T] = _getMessages(List.empty[Message], List.empty[T], validator, maxMessages / 10 + 10)

  def deleteMessages(messages: Message*): Unit = messages.foreach(message => sqs.deleteMessage(queueUrl, message.getReceiptHandle))
}

object SqsClient {
  class SqsClientBuilder private (queueUrl: String, credentials: Option[AWSCredentialsProvider], region: Option[Regions], minMessages: Int, maxMessages: Int) {
    def this(queueUrl: String) = this(queueUrl, None, None, 0, Int.MaxValue)

    def getQueueUrl = queueUrl
    def getCredentials = credentials
    def getRegion = region
    def getMinMessages = minMessages
    def getMaxMessages = maxMessages

    def withCredentials(accessKey: String, accessSecret: String) : SqsClientBuilder = {
      val credentials = new BasicAWSCredentialsProvider(accessKey, accessSecret)
      new SqsClientBuilder(queueUrl, Some(credentials), region, minMessages, maxMessages)
    }

    def withMinimumMessages(minimumMessages: Int) : SqsClientBuilder = {
      val min = Math.max(0, minimumMessages)
      val max = Math.max(min, maxMessages)

      new SqsClientBuilder(queueUrl, credentials, region, min, max)
    }

    def withMaximumMessages(maximumMessages: Int) : SqsClientBuilder = {
      val max = Math.max(1, maximumMessages)
      val min = Math.min(minMessages, max)

      new SqsClientBuilder(queueUrl, credentials, region, min, max)
    }

    def withRegion(region: Option[Regions]) : SqsClientBuilder = {
      new SqsClientBuilder(queueUrl, credentials, region, minMessages, maxMessages)
    }

    def build(): SqsClient = SqsClient.apply(this)
  }

  private def apply(builder: SqsClientBuilder): SqsClient = {
    val sqsClient = if(builder.getCredentials.isDefined) {
      new AmazonSQSClient(builder.getCredentials.get)
    } else {
      new AmazonSQSClient()
    }

    new SqsClient(sqsClient, builder.getQueueUrl, builder.getMinMessages, builder.getMaxMessages)
  }
}