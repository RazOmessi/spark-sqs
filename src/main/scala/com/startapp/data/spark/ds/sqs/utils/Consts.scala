package com.startapp.data.spark.ds.sqs.utils

import com.startapp.data.spark.ds.sqs.client.CommitPolicy

object Consts {
  val QUEUE_URL = "queueURL"
  val REGION = "region"
  val ACCESS_KEY = "accessKey"
  val ACCESS_SECRET = "accessSecret"

  val MIN_MESSAGES = "minMessages"
  val MAX_MESSAGES = "maxMessages"
  val FILE_PREFIX = "filePrefix"

  val INNER_FORMAT = "innerFormat"

  val MESSAGE_PARSER_CLASS = "messageParserClass"
  val S3_MESSAGE_PARSER_CLASS = "com.startapp.data.spark.ds.sqs.messages.SqsS3FileMessageReader"

  val DELETE_POLICY = "deletePolicy"
  val DELETE_POLICYS = Map (
    CommitPolicy.Never -> "com.startapp.data.spark.ds.sqs.client.SqsNeverDeletePolicy",
    CommitPolicy.QuerySuccess -> "com.startapp.data.spark.ds.sqs.client.SqsQuerySuccessDeletePolicy",
    CommitPolicy.Shutdown -> "com.startapp.data.spark.ds.sqs.client.SqsShutdownDeletePolicy"
  )

  val DELETE_POLICY_CLASS = "deletePolicyClass"
  val QUERY_SUCCESS_COMMIT_CLASS = "com.startapp.data.spark.ds.sqs.client.SqsQuerySuccessCommit"


}
