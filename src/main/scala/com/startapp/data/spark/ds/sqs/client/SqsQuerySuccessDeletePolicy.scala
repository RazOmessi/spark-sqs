package com.startapp.data.spark.ds.sqs.client

import com.amazonaws.services.sqs.model.Message
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SqsQuerySuccessDeletePolicy(sqsClient: SqsClient, messages: List[Message]) extends AbstractSqsDeletePolicy(sqsClient, messages) {
  private val logger = Logger.getLogger(classOf[SqsQuerySuccessDeletePolicy])

  override def registerCommitMessages(sqlContext: SQLContext): Unit = {
    val listener = new QueryExecutionListener {
      logger.info("SQS messages will commit on query success (using SqsQuerySuccessCommit as a commit class).")

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        logger.info("query failed. not committing SQS messages.")
        sqlContext.sparkSession.listenerManager.unregister(this)
      }

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        logger.info("query succeeded. committing SQS messages.")
        sqlContext.sparkSession.listenerManager.unregister(this)
        commitMessages()
      }
    }

    sqlContext.sparkSession.listenerManager.register(listener)
  }
}
