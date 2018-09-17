package com.startapp.data.spark.ds.sqs.client

import com.amazonaws.services.sqs.model.Message
import org.apache.spark.sql.SQLContext

abstract class AbstractSqsDeletePolicy(sqsClient: SqsClient, messages: List[Message]) {
  protected def commitMessages(): Unit = {
    sqsClient.deleteMessages(messages : _*)
  }

  def registerCommitMessages(sqlContext: SQLContext): Unit
}
