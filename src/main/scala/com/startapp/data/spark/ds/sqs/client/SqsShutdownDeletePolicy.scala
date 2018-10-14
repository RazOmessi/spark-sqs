package com.startapp.data.spark.ds.sqs.client

import com.amazonaws.services.sqs.model.Message
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SqsShutdownDeletePolicy(sqsClient: SqsClient, messages: List[Message]) extends AbstractSqsDeletePolicy(sqsClient, messages) {
  private val logger = Logger.getLogger(classOf[SqsShutdownDeletePolicy])
  private var flag = true

  override def registerCommitMessages(sqlContext: SQLContext): Unit = {
    logger.info("SQS messages will commit on shutdown (using SqsShutdownCommit as a commit class).")

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      val default = Thread.getDefaultUncaughtExceptionHandler

      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        logger.error("Caught uncaught exception, flagging job as failed and messages will not be deleted from SQS.", e)
        flag = false
        default.uncaughtException(t, e)
      }
    })

    sqlContext.listenerManager.register(new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        flag = false
        logger.error(s"Spark query failed, flagging job as failed and messages will not be deleted from SQS.")
      }

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {}
    })

    val hook = new Thread{
      override def run(): Unit = {
        if(flag){
          logger.info("application shutdown. committing SQS messages.")
          commitMessages()
        } else {
          logger.info("application shutdown. messages will not be deleted from SQS.")
        }
      }
    }
    Runtime.getRuntime.addShutdownHook(hook)
  }
}
