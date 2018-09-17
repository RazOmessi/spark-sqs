package com.startapp.data.spark.ds.sqs.client

import com.amazonaws.services.sqs.model.Message
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext

class SqsNeverDeletePolicy(sqsClient: SqsClient, messages: List[Message]) extends AbstractSqsDeletePolicy(sqsClient, messages) {
  private val logger = Logger.getLogger(classOf[SqsNeverDeletePolicy])

  override def registerCommitMessages(sqlContext: SQLContext): Unit = {
    logger.info("Sqs messages will never commit (using SqsNeverCommit as a commit class).")
  }
}
